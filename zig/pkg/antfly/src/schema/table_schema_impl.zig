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
const schema_regex = @import("antfly_regex");
const storage_schema = @import("../storage/schema.zig");

pub const StorageMode = enum {
    document,
    relational,

    pub fn fromString(text: []const u8) ?StorageMode {
        if (std.mem.eql(u8, text, "document")) return .document;
        if (std.mem.eql(u8, text, "relational")) return .relational;
        return null;
    }
};

pub const RelationalIndexLifecycle = enum {
    ready,
    building,
    invalid,
    dropping,
    catching_up,
    stale,
    rebuild_required,
    failed,

    pub fn fromString(text: []const u8) ?RelationalIndexLifecycle {
        if (std.mem.eql(u8, text, "ready")) return .ready;
        if (std.mem.eql(u8, text, "building")) return .building;
        if (std.mem.eql(u8, text, "invalid")) return .invalid;
        if (std.mem.eql(u8, text, "dropping")) return .dropping;
        if (std.mem.eql(u8, text, "catching_up")) return .catching_up;
        if (std.mem.eql(u8, text, "stale")) return .stale;
        if (std.mem.eql(u8, text, "rebuild_required")) return .rebuild_required;
        if (std.mem.eql(u8, text, "failed")) return .failed;
        return null;
    }
};

fn storageRelationalIndexLifecycleFromString(text: []const u8) ?storage_schema.RelationalIndexLifecycle {
    const parsed = RelationalIndexLifecycle.fromString(text) orelse return null;
    return switch (parsed) {
        .ready => .ready,
        .building => .building,
        .invalid => .invalid,
        .dropping => .dropping,
        .catching_up => .catching_up,
        .stale => .stale,
        .rebuild_required => .rebuild_required,
        .failed => .failed,
    };
}

pub const TableSchema = struct {
    version: u32 = 0,
    storage_mode: StorageMode = .document,
    default_type: []const u8 = "",
    ttl_duration_ns: u64 = 0,
    ttl_field: []const u8 = "_timestamp",
    enforce_types: bool = false,
    document_schemas: []DocumentSchema = &.{},
    dynamic_templates: []DynamicTemplate = &.{},
    primary_key: ?PrimaryKey = null,
    periods: []RelationalPeriod = &.{},
    foreign_keys: []ForeignKey = &.{},
    unique_constraints: []UniqueConstraint = &.{},
    relational_indexes: []storage_schema.RelationalIndex = &.{},
    checks: []RelationalCheck = &.{},
    external_base_source: ?storage_schema.ExternalBaseSource = null,
    system_versioned: bool = false,

    pub fn deinit(self: *TableSchema, alloc: std.mem.Allocator) void {
        alloc.free(self.default_type);
        alloc.free(self.ttl_field);
        for (self.document_schemas) |*document_schema| document_schema.deinit(alloc);
        if (self.document_schemas.len > 0) alloc.free(self.document_schemas);
        for (self.dynamic_templates) |*dynamic_template| dynamic_template.deinit(alloc);
        if (self.dynamic_templates.len > 0) alloc.free(self.dynamic_templates);
        if (self.primary_key) |*primary_key| primary_key.deinit(alloc);
        for (self.periods) |*period| period.deinit(alloc);
        if (self.periods.len > 0) alloc.free(self.periods);
        for (self.foreign_keys) |*foreign_key| foreign_key.deinit(alloc);
        if (self.foreign_keys.len > 0) alloc.free(self.foreign_keys);
        for (self.unique_constraints) |*constraint| constraint.deinit(alloc);
        if (self.unique_constraints.len > 0) alloc.free(self.unique_constraints);
        for (self.relational_indexes) |index| freeParsedRelationalIndex(alloc, index);
        if (self.relational_indexes.len > 0) alloc.free(self.relational_indexes);
        for (self.checks) |*check| check.deinit(alloc);
        if (self.checks.len > 0) alloc.free(self.checks);
        if (self.external_base_source) |source| storage_schema.freeExternalBaseSource(alloc, source);
        self.* = undefined;
    }
};

pub const PrimaryKey = struct {
    name: ?[]const u8 = null,
    columns: [][]const u8 = &.{},
    include_columns: [][]const u8 = &.{},
    without_overlaps_period: ?[]const u8 = null,
    deferrable: bool = false,
    timing: ForeignKeyTiming = .immediate,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.name) |name| alloc.free(name);
        for (self.columns) |column| alloc.free(column);
        if (self.columns.len > 0) alloc.free(self.columns);
        for (self.include_columns) |column| alloc.free(column);
        if (self.include_columns.len > 0) alloc.free(self.include_columns);
        if (self.without_overlaps_period) |period| alloc.free(period);
        self.* = undefined;
    }
};

pub const RelationalPeriod = struct {
    name: []const u8,
    start_column: []const u8,
    end_column: []const u8,
    range_type: ?storage_schema.RelationalPeriodRangeType = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        alloc.free(self.start_column);
        alloc.free(self.end_column);
        self.* = undefined;
    }
};

pub const ForeignKeyAction = enum {
    restrict,
    no_action,
    set_null,
    cascade,

    pub fn fromString(text: []const u8) ?ForeignKeyAction {
        if (enumTokenEql(text, "restrict") or enumTokenEql(text, "delete_restrict") or enumTokenEql(text, "update_restrict") or enumTokenEql(text, "on_delete_restrict") or enumTokenEql(text, "on_update_restrict")) return .restrict;
        if (enumTokenEql(text, "no_action") or enumTokenEql(text, "delete_no_action") or enumTokenEql(text, "update_no_action") or enumTokenEql(text, "on_delete_no_action") or enumTokenEql(text, "on_update_no_action")) return .no_action;
        if (enumTokenEql(text, "set_null") or enumTokenEql(text, "delete_set_null") or enumTokenEql(text, "update_set_null") or enumTokenEql(text, "on_delete_set_null") or enumTokenEql(text, "on_update_set_null")) return .set_null;
        if (enumTokenEql(text, "cascade") or enumTokenEql(text, "delete_cascade") or enumTokenEql(text, "update_cascade") or enumTokenEql(text, "on_delete_cascade") or enumTokenEql(text, "on_update_cascade")) return .cascade;
        return null;
    }
};

pub const ForeignKeyTiming = enum {
    immediate,
    deferred,

    pub fn fromString(text: []const u8) ?ForeignKeyTiming {
        if (enumTokenEql(text, "immediate") or
            enumTokenEql(text, "initially_immediate") or
            enumTokenEql(text, "deferrable_initially_immediate") or
            enumTokenEql(text, "not_deferrable_initially_immediate"))
        {
            return .immediate;
        }
        if (enumTokenEql(text, "deferred") or
            enumTokenEql(text, "initially_deferred") or
            enumTokenEql(text, "deferrable_initially_deferred") or
            enumTokenEql(text, "not_deferrable_initially_deferred"))
        {
            return .deferred;
        }
        return null;
    }
};

pub const ForeignKeyMatch = enum {
    simple,
    full,
    partial,

    pub fn fromString(text: []const u8) ?ForeignKeyMatch {
        if (enumTokenEql(text, "simple") or enumTokenEql(text, "match_simple")) return .simple;
        if (enumTokenEql(text, "full") or enumTokenEql(text, "match_full")) return .full;
        if (enumTokenEql(text, "partial") or enumTokenEql(text, "match_partial")) return .partial;
        return null;
    }
};

pub const ForeignKeyValidationState = enum {
    enforced,
    unvalidated,
    validating,
    invalid,

    pub fn fromString(text: []const u8) ?ForeignKeyValidationState {
        if (enumTokenEql(text, "enforced")) return .enforced;
        if (enumTokenEql(text, "unvalidated") or enumTokenEql(text, "not_valid")) return .unvalidated;
        if (enumTokenEql(text, "validating")) return .validating;
        if (enumTokenEql(text, "invalid")) return .invalid;
        return null;
    }
};

pub const UniqueConstraintValidationState = enum {
    enforced,
    unvalidated,
    validating,
    invalid,

    pub fn fromString(text: []const u8) ?UniqueConstraintValidationState {
        if (enumTokenEql(text, "enforced")) return .enforced;
        if (enumTokenEql(text, "unvalidated") or enumTokenEql(text, "not_valid")) return .unvalidated;
        if (enumTokenEql(text, "validating")) return .validating;
        if (enumTokenEql(text, "invalid")) return .invalid;
        return null;
    }
};

fn foreignKeyDeferrableFromValue(value: std.json.Value) ?bool {
    return if (foreignKeyDeferrabilityFromValue(value)) |clause| clause.deferrable else null;
}

const ForeignKeyDeferrability = struct {
    deferrable: bool,
    timing: ?ForeignKeyTiming = null,
};

const ForeignKeyTimingClause = struct {
    timing: ForeignKeyTiming,
    deferrable: ?bool = null,
};

fn foreignKeyTimingClauseFromString(text: []const u8) ?ForeignKeyTimingClause {
    const timing = ForeignKeyTiming.fromString(text) orelse return null;
    const deferrable = if (foreignKeyDeferrableFromString(text)) |clause| clause.deferrable else null;
    return .{ .timing = timing, .deferrable = deferrable };
}

fn foreignKeyDeferrabilityFromValue(value: std.json.Value) ?ForeignKeyDeferrability {
    return switch (value) {
        .bool => |enabled| .{ .deferrable = enabled },
        .string => |text| foreignKeyDeferrableFromString(text),
        else => null,
    };
}

fn foreignKeyDeferrableFromString(text: []const u8) ?ForeignKeyDeferrability {
    if (enumTokenEql(text, "deferrable")) return .{ .deferrable = true };
    if (enumTokenEql(text, "not_deferrable")) return .{ .deferrable = false };
    if (enumTokenEql(text, "deferrable_initially_immediate")) return .{ .deferrable = true, .timing = .immediate };
    if (enumTokenEql(text, "deferrable_initially_deferred")) return .{ .deferrable = true, .timing = .deferred };
    if (enumTokenEql(text, "not_deferrable_initially_immediate")) return .{ .deferrable = false, .timing = .immediate };
    if (enumTokenEql(text, "not_deferrable_initially_deferred")) return .{ .deferrable = false, .timing = .deferred };
    return null;
}

const ConstraintTimingMetadata = struct {
    timing: ForeignKeyTiming = .immediate,
    deferrable: bool = false,
};

fn constraintTimingMetadataFromObject(object: std.json.ObjectMap) !ConstraintTimingMetadata {
    const timing_clause = if (object.get("timing")) |timing_value|
        foreignKeyTimingClauseFromString(timing_value.string).?
    else
        null;
    const explicit_timing = if (timing_clause) |clause| clause.timing else null;
    const deferrability = if (object.get("deferrable")) |deferrable_value|
        foreignKeyDeferrabilityFromValue(deferrable_value).?
    else
        null;
    const timing_from_deferrability = if (deferrability) |clause| clause.timing else null;
    if (explicit_timing != null and timing_from_deferrability != null and explicit_timing.? != timing_from_deferrability.?) {
        return error.InvalidSchemaUpdateRequest;
    }
    const explicit_deferrable = if (timing_clause) |clause| clause.deferrable else null;
    const deferrable_from_deferrability = if (deferrability) |clause| clause.deferrable else null;
    if (explicit_deferrable != null and deferrable_from_deferrability != null and explicit_deferrable.? != deferrable_from_deferrability.?) {
        return error.InvalidSchemaUpdateRequest;
    }
    const timing = explicit_timing orelse timing_from_deferrability orelse ForeignKeyTiming.immediate;
    const deferrable = explicit_deferrable orelse deferrable_from_deferrability orelse (timing == .deferred);
    if (timing == .deferred and !deferrable) return error.InvalidSchemaUpdateRequest;
    return .{ .timing = timing, .deferrable = deferrable };
}

fn enumTokenEql(actual: []const u8, expected: []const u8) bool {
    var actual_index: usize = 0;
    var expected_index: usize = 0;
    while (true) {
        while (actual_index < actual.len and enumTokenSeparator(actual[actual_index])) actual_index += 1;
        while (expected_index < expected.len and enumTokenSeparator(expected[expected_index])) expected_index += 1;
        if (actual_index == actual.len or expected_index == expected.len) break;
        if (std.ascii.toLower(actual[actual_index]) != std.ascii.toLower(expected[expected_index])) return false;
        actual_index += 1;
        expected_index += 1;
    }
    while (actual_index < actual.len and enumTokenSeparator(actual[actual_index])) actual_index += 1;
    while (expected_index < expected.len and enumTokenSeparator(expected[expected_index])) expected_index += 1;
    return actual_index == actual.len and expected_index == expected.len;
}

fn enumTokenSeparator(ch: u8) bool {
    return std.ascii.isWhitespace(ch) or ch == '_' or ch == '-';
}

fn isSqlColumnAliasIdentifier(value: []const u8) bool {
    if (value.len == 0) return false;
    if (!std.ascii.isAlphabetic(value[0]) and value[0] != '_') return false;
    for (value[1..]) |ch| {
        if (!std.ascii.isAlphanumeric(ch) and ch != '_') return false;
    }
    return true;
}

pub const ForeignKeyReference = struct {
    table: []const u8,
    columns: [][]const u8 = &.{},
    period: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table);
        for (self.columns) |column| alloc.free(column);
        if (self.columns.len > 0) alloc.free(self.columns);
        if (self.period) |period| alloc.free(period);
        self.* = undefined;
    }
};

pub const ForeignKey = struct {
    name: []const u8,
    columns: [][]const u8 = &.{},
    period: ?[]const u8 = null,
    references: ForeignKeyReference,
    on_delete: ForeignKeyAction = .restrict,
    on_update: ForeignKeyAction = .restrict,
    timing: ForeignKeyTiming = .immediate,
    deferrable: bool = false,
    match: ForeignKeyMatch = .simple,
    validation_state: ForeignKeyValidationState = .enforced,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        for (self.columns) |column| alloc.free(column);
        if (self.columns.len > 0) alloc.free(self.columns);
        if (self.period) |period| alloc.free(period);
        self.references.deinit(alloc);
        self.* = undefined;
    }
};

pub const UniqueConstraint = struct {
    name: []const u8,
    columns: [][]const u8 = &.{},
    expressions: []UniqueExpression = &.{},
    include_columns: [][]const u8 = &.{},
    without_overlaps_period: ?[]const u8 = null,
    nulls_not_distinct: bool = false,
    deferrable: bool = false,
    timing: ForeignKeyTiming = .immediate,
    where: []UniquePredicate = &.{},
    where_expressions: []storage_schema.RelationalRowsExpressionCondition = &.{},
    validation_state: UniqueConstraintValidationState = .enforced,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        for (self.columns) |column| alloc.free(column);
        if (self.columns.len > 0) alloc.free(self.columns);
        for (self.expressions) |expression| {
            alloc.free(expression.field);
            if (expression.expression) |row_expression| freeRelationalRowsExpression(alloc, row_expression);
        }
        if (self.expressions.len > 0) alloc.free(self.expressions);
        for (self.include_columns) |column| alloc.free(column);
        if (self.include_columns.len > 0) alloc.free(self.include_columns);
        if (self.without_overlaps_period) |period| alloc.free(period);
        for (self.where) |predicate| {
            alloc.free(predicate.field);
            if (predicate.value_json) |value| alloc.free(value);
        }
        if (self.where.len > 0) alloc.free(self.where);
        for (self.where_expressions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
        if (self.where_expressions.len > 0) alloc.free(self.where_expressions);
        self.* = undefined;
    }
};

pub const UniqueExpressionOp = enum {
    lower,
    upper,
    md5,
    expression,
};

fn freeRelationalIndexKeys(alloc: std.mem.Allocator, keys: []const storage_schema.RelationalIndexKey) void {
    for (keys) |key| alloc.free(key.column);
    if (keys.len > 0) alloc.free(keys);
}

fn freeParsedRelationalIndex(alloc: std.mem.Allocator, index: storage_schema.RelationalIndex) void {
    alloc.free(index.name);
    alloc.free(index.owner_name);
    if (index.method_config_json) |config| alloc.free(config);
    freeStringSlice(alloc, index.columns);
    freeStorageUniqueExpressions(alloc, index.expressions);
    freeStringSlice(alloc, index.include_columns);
    freeRelationalIndexKeys(alloc, index.keys);
    if (index.schema_fingerprint) |fingerprint| alloc.free(fingerprint);
    freeStorageUniquePredicates(alloc, index.where);
    for (index.where_expressions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
    if (index.where_expressions.len > 0) alloc.free(index.where_expressions);
}

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

fn freeStorageUniqueExpressions(alloc: std.mem.Allocator, values: []const storage_schema.UniqueExpression) void {
    for (values) |expression| {
        alloc.free(expression.field);
        if (expression.expression) |row_expression| freeRelationalRowsExpression(alloc, row_expression);
    }
    if (values.len > 0) alloc.free(values);
}

fn freeStorageUniquePredicates(alloc: std.mem.Allocator, values: []const storage_schema.UniquePredicate) void {
    for (values) |predicate| {
        alloc.free(predicate.field);
        if (predicate.value_json) |value_json| alloc.free(value_json);
    }
    if (values.len > 0) alloc.free(values);
}

pub const UniqueExpression = struct {
    op: UniqueExpressionOp,
    field: []const u8 = "",
    expression: ?storage_schema.RelationalRowsExpression = null,
};

pub const UniquePredicateOp = enum {
    is_null,
    is_not_null,
    eq,
    ne,
};

pub const UniquePredicate = struct {
    field: []const u8,
    op: UniquePredicateOp,
    value_json: ?[]const u8 = null,
};

pub const RelationalCheckOp = enum {
    is_null,
    is_not_null,
    is_distinct,
    is_not_distinct,
    eq,
    ne,
    gt,
    gte,
    lt,
    lte,
};

pub const RelationalCheck = struct {
    name: []const u8,
    field: []const u8 = "",
    op: RelationalCheckOp = .eq,
    value_json: ?[]const u8 = null,
    collation: ?[]const u8 = null,
    validation_state: RelationalCheckValidationState = .enforced,
    expression: ?storage_schema.RelationalRowsExpressionCondition = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        alloc.free(self.field);
        if (self.value_json) |value| alloc.free(value);
        if (self.collation) |value| alloc.free(value);
        if (self.expression) |expression| freeRelationalRowsExpressionCondition(alloc, expression);
        self.* = undefined;
    }
};

pub const RelationalCheckValidationState = enum {
    enforced,
    unvalidated,
    validating,
    invalid,
};

const RelationalRowsExpressionType = enum {
    text,
    numeric,
    boolean,
    datetime,
    json,
    array,
    null,
};

pub const RelationalGeneratedOp = enum {
    lower,
    upper,
    md5,
    concat,
    concat_ws,
    expression,
};

pub const RelationalDefaultKind = enum {
    literal,
    now_ns,
    current_date_ns,
    uuid_v4,
    sequence_next,
    scalar_subquery,
};

pub const RelationalDefaultValue = struct {
    kind: RelationalDefaultKind = .literal,
    value_json: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.value_json);
        self.* = undefined;
    }
};

pub const RelationalGeneratedValue = struct {
    op: RelationalGeneratedOp,
    field: ?[]const u8 = null,
    fields: [][]const u8 = &.{},
    separator: []const u8 = "",
    expression: ?storage_schema.RelationalRowsExpression = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.field) |field| alloc.free(field);
        for (self.fields) |field| alloc.free(field);
        if (self.fields.len > 0) alloc.free(self.fields);
        alloc.free(self.separator);
        if (self.expression) |expression| freeRelationalRowsExpression(alloc, expression);
        self.* = undefined;
    }
};

pub const DocumentSchema = struct {
    name: []const u8,
    min_properties: ?u64 = null,
    max_properties: ?u64 = null,
    required_fields: [][]const u8 = &.{},
    include_in_all_fields: [][]const u8 = &.{},
    properties: []DocumentProperty = &.{},
    pattern_properties: []const PatternProperty = &.{},
    additional_properties_allowed: ?bool = null,
    additional_properties_schema: ?*DocumentProperty = null,
    dynamic_infer_types: bool = false,
    unevaluated_properties_allowed: ?bool = null,
    unevaluated_properties_schema: ?*DocumentProperty = null,
    property_names: ?*DocumentProperty = null,
    dependent_required: []const DependentRequired = &.{},
    dependent_schemas: []const DependentSchema = &.{},
    any_of: []DocumentProperty = &.{},
    one_of: []DocumentProperty = &.{},
    all_of: []DocumentProperty = &.{},
    not_schema: ?*DocumentProperty = null,
    if_schema: ?*DocumentProperty = null,
    then_schema: ?*DocumentProperty = null,
    else_schema: ?*DocumentProperty = null,

    pub fn deinit(self: *DocumentSchema, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        for (self.required_fields) |field_name| alloc.free(field_name);
        if (self.required_fields.len > 0) alloc.free(self.required_fields);
        for (self.include_in_all_fields) |field_name| alloc.free(field_name);
        if (self.include_in_all_fields.len > 0) alloc.free(self.include_in_all_fields);
        for (self.properties) |*property| property.deinit(alloc);
        if (self.properties.len > 0) alloc.free(self.properties);
        for (self.pattern_properties) |property| {
            var owned = property;
            owned.deinit(alloc);
        }
        if (self.pattern_properties.len > 0) alloc.free(self.pattern_properties);
        if (self.additional_properties_schema) |additional_properties_schema| {
            additional_properties_schema.deinit(alloc);
            alloc.destroy(additional_properties_schema);
        }
        if (self.unevaluated_properties_schema) |unevaluated_properties_schema| {
            unevaluated_properties_schema.deinit(alloc);
            alloc.destroy(unevaluated_properties_schema);
        }
        if (self.property_names) |property_names| {
            property_names.deinit(alloc);
            alloc.destroy(property_names);
        }
        for (self.dependent_required) |dependency| {
            var owned = dependency;
            owned.deinit(alloc);
        }
        if (self.dependent_required.len > 0) alloc.free(self.dependent_required);
        for (self.dependent_schemas) |dependency| {
            var owned = dependency;
            owned.deinit(alloc);
        }
        if (self.dependent_schemas.len > 0) alloc.free(self.dependent_schemas);
        for (self.any_of) |*property| property.deinit(alloc);
        if (self.any_of.len > 0) alloc.free(self.any_of);
        for (self.one_of) |*property| property.deinit(alloc);
        if (self.one_of.len > 0) alloc.free(self.one_of);
        for (self.all_of) |*property| property.deinit(alloc);
        if (self.all_of.len > 0) alloc.free(self.all_of);
        if (self.not_schema) |not_schema| {
            not_schema.deinit(alloc);
            alloc.destroy(not_schema);
        }
        if (self.if_schema) |if_schema| {
            if_schema.deinit(alloc);
            alloc.destroy(if_schema);
        }
        if (self.then_schema) |then_schema| {
            then_schema.deinit(alloc);
            alloc.destroy(then_schema);
        }
        if (self.else_schema) |else_schema| {
            else_schema.deinit(alloc);
            alloc.destroy(else_schema);
        }
        self.* = undefined;
    }
};

pub const DocumentProperty = struct {
    name: []const u8,
    root_ref: bool = false,
    field_type: ?[]const u8 = null,
    antfly_types: [][]const u8 = &.{},
    sql_column_name: ?[]const u8 = null,
    analyzer: ?[]const u8 = null,
    collation: ?[]const u8 = null,
    antfly_index: ?bool = null,
    index_lifecycle: ?RelationalIndexLifecycle = null,
    index_generation: ?u64 = null,
    index_name: ?[]const u8 = null,
    index_access_method: ?storage_schema.RelationalIndexAccessMethod = null,
    index_schema_fingerprint: ?[]const u8 = null,
    index_include_columns: [][]const u8 = &.{},
    index_keys: []storage_schema.RelationalIndexKey = &.{},
    cardinality_proof: storage_schema.RelationalColumnCardinalityProof = .none,
    integer_only: bool = false,
    format: ?[]const u8 = null,
    allows_null: bool = false,
    const_value: ?[]const u8 = null,
    minimum: ?f64 = null,
    maximum: ?f64 = null,
    exclusive_minimum: ?f64 = null,
    exclusive_maximum: ?f64 = null,
    multiple_of: ?f64 = null,
    min_length: ?u64 = null,
    max_length: ?u64 = null,
    min_properties: ?u64 = null,
    max_properties: ?u64 = null,
    pattern: ?[]const u8 = null,
    min_items: ?u64 = null,
    max_items: ?u64 = null,
    additional_items_allowed: ?bool = null,
    min_contains: ?u64 = null,
    max_contains: ?u64 = null,
    unique_items: bool = false,
    enum_values: [][]const u8 = &.{},
    required_fields: [][]const u8 = &.{},
    include_in_all_fields: [][]const u8 = &.{},
    prefix_items: []DocumentProperty = &.{},
    properties: []DocumentProperty = &.{},
    pattern_properties: []const PatternProperty = &.{},
    additional_properties_allowed: ?bool = null,
    additional_properties_schema: ?*DocumentProperty = null,
    dynamic_infer_types: bool = false,
    unevaluated_properties_allowed: ?bool = null,
    unevaluated_properties_schema: ?*DocumentProperty = null,
    property_names: ?*DocumentProperty = null,
    dependent_required: []const DependentRequired = &.{},
    dependent_schemas: []const DependentSchema = &.{},
    any_of: []DocumentProperty = &.{},
    one_of: []DocumentProperty = &.{},
    all_of: []DocumentProperty = &.{},
    not_schema: ?*DocumentProperty = null,
    if_schema: ?*DocumentProperty = null,
    then_schema: ?*DocumentProperty = null,
    else_schema: ?*DocumentProperty = null,
    contains_schema: ?*DocumentProperty = null,
    item: ?*DocumentProperty = null,
    unevaluated_items_allowed: ?bool = null,
    unevaluated_items_schema: ?*DocumentProperty = null,
    embedded_schema: ?*DocumentProperty = null,
    embedded_dynamic_templates: []DynamicTemplate = &.{},
    default_value: ?RelationalDefaultValue = null,
    on_update_value: ?RelationalDefaultValue = null,
    generated: ?RelationalGeneratedValue = null,
    index_where: []UniquePredicate = &.{},
    index_where_expressions: []storage_schema.RelationalRowsExpressionCondition = &.{},

    pub fn deinit(self: *DocumentProperty, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        if (self.field_type) |field_type| alloc.free(field_type);
        for (self.antfly_types) |antfly_type| alloc.free(antfly_type);
        if (self.antfly_types.len > 0) alloc.free(self.antfly_types);
        if (self.sql_column_name) |column_name| alloc.free(column_name);
        if (self.analyzer) |analyzer| alloc.free(analyzer);
        if (self.collation) |collation| alloc.free(collation);
        if (self.index_name) |index_name| alloc.free(index_name);
        if (self.index_schema_fingerprint) |fingerprint| alloc.free(fingerprint);
        for (self.index_include_columns) |field_name| alloc.free(field_name);
        if (self.index_include_columns.len > 0) alloc.free(self.index_include_columns);
        freeRelationalIndexKeys(alloc, self.index_keys);
        if (self.format) |format| alloc.free(format);
        if (self.const_value) |const_value| alloc.free(const_value);
        if (self.pattern) |pattern| alloc.free(pattern);
        for (self.enum_values) |enum_value| alloc.free(enum_value);
        if (self.enum_values.len > 0) alloc.free(self.enum_values);
        for (self.required_fields) |field_name| alloc.free(field_name);
        if (self.required_fields.len > 0) alloc.free(self.required_fields);
        for (self.include_in_all_fields) |field_name| alloc.free(field_name);
        if (self.include_in_all_fields.len > 0) alloc.free(self.include_in_all_fields);
        for (self.prefix_items) |*property| property.deinit(alloc);
        if (self.prefix_items.len > 0) alloc.free(self.prefix_items);
        for (self.properties) |*property| property.deinit(alloc);
        if (self.properties.len > 0) alloc.free(self.properties);
        for (self.pattern_properties) |property| {
            var owned = property;
            owned.deinit(alloc);
        }
        if (self.pattern_properties.len > 0) alloc.free(self.pattern_properties);
        if (self.additional_properties_schema) |additional_properties_schema| {
            additional_properties_schema.deinit(alloc);
            alloc.destroy(additional_properties_schema);
        }
        if (self.unevaluated_properties_schema) |unevaluated_properties_schema| {
            unevaluated_properties_schema.deinit(alloc);
            alloc.destroy(unevaluated_properties_schema);
        }
        if (self.property_names) |property_names| {
            property_names.deinit(alloc);
            alloc.destroy(property_names);
        }
        for (self.dependent_required) |dependency| {
            var owned = dependency;
            owned.deinit(alloc);
        }
        if (self.dependent_required.len > 0) alloc.free(self.dependent_required);
        for (self.dependent_schemas) |dependency| {
            var owned = dependency;
            owned.deinit(alloc);
        }
        if (self.dependent_schemas.len > 0) alloc.free(self.dependent_schemas);
        for (self.any_of) |*property| property.deinit(alloc);
        if (self.any_of.len > 0) alloc.free(self.any_of);
        for (self.one_of) |*property| property.deinit(alloc);
        if (self.one_of.len > 0) alloc.free(self.one_of);
        for (self.all_of) |*property| property.deinit(alloc);
        if (self.all_of.len > 0) alloc.free(self.all_of);
        if (self.not_schema) |not_schema| {
            not_schema.deinit(alloc);
            alloc.destroy(not_schema);
        }
        if (self.if_schema) |if_schema| {
            if_schema.deinit(alloc);
            alloc.destroy(if_schema);
        }
        if (self.then_schema) |then_schema| {
            then_schema.deinit(alloc);
            alloc.destroy(then_schema);
        }
        if (self.else_schema) |else_schema| {
            else_schema.deinit(alloc);
            alloc.destroy(else_schema);
        }
        if (self.contains_schema) |contains_schema| {
            contains_schema.deinit(alloc);
            alloc.destroy(contains_schema);
        }
        if (self.item) |item| {
            item.deinit(alloc);
            alloc.destroy(item);
        }
        if (self.unevaluated_items_schema) |unevaluated_items_schema| {
            unevaluated_items_schema.deinit(alloc);
            alloc.destroy(unevaluated_items_schema);
        }
        if (self.embedded_schema) |embedded_schema| {
            embedded_schema.deinit(alloc);
            alloc.destroy(embedded_schema);
        }
        for (self.embedded_dynamic_templates) |*dynamic_template| dynamic_template.deinit(alloc);
        if (self.embedded_dynamic_templates.len > 0) alloc.free(self.embedded_dynamic_templates);
        if (self.default_value) |*value| value.deinit(alloc);
        if (self.on_update_value) |*value| value.deinit(alloc);
        if (self.generated) |*generated| generated.deinit(alloc);
        for (self.index_where) |predicate| {
            alloc.free(predicate.field);
            if (predicate.value_json) |value| alloc.free(value);
        }
        if (self.index_where.len > 0) alloc.free(self.index_where);
        for (self.index_where_expressions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
        if (self.index_where_expressions.len > 0) alloc.free(self.index_where_expressions);
        self.* = undefined;
    }
};

pub const DependentRequired = struct {
    name: []const u8,
    required_fields: [][]const u8 = &.{},

    pub fn deinit(self: *DependentRequired, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        for (self.required_fields) |field_name| alloc.free(field_name);
        if (self.required_fields.len > 0) alloc.free(self.required_fields);
        self.* = undefined;
    }
};

pub const DependentSchema = struct {
    name: []const u8,
    schema: *DocumentProperty,

    pub fn deinit(self: *DependentSchema, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        self.schema.deinit(alloc);
        alloc.destroy(self.schema);
        self.* = undefined;
    }
};

pub const PatternProperty = struct {
    pattern: []const u8,
    property: *DocumentProperty,

    pub fn deinit(self: *PatternProperty, alloc: std.mem.Allocator) void {
        alloc.free(self.pattern);
        self.property.deinit(alloc);
        alloc.destroy(self.property);
        self.* = undefined;
    }
};

pub const DynamicTemplate = struct {
    name: []const u8,
    match_pattern: ?[]const u8 = null,
    unmatch_pattern: ?[]const u8 = null,
    path_match: ?[]const u8 = null,
    path_unmatch: ?[]const u8 = null,
    match_mapping_type: ?[]const u8 = null,
    field_type: ?[]const u8 = null,
    analyzer: ?[]const u8 = null,
    do_index: ?bool = null,
    store: ?bool = null,
    doc_values: ?bool = null,
    include_in_all: ?bool = null,

    pub fn deinit(self: *DynamicTemplate, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        if (self.match_pattern) |match_pattern| alloc.free(match_pattern);
        if (self.unmatch_pattern) |unmatch_pattern| alloc.free(unmatch_pattern);
        if (self.path_match) |path_match| alloc.free(path_match);
        if (self.path_unmatch) |path_unmatch| alloc.free(path_unmatch);
        if (self.match_mapping_type) |match_mapping_type| alloc.free(match_mapping_type);
        if (self.field_type) |field_type| alloc.free(field_type);
        if (self.analyzer) |analyzer| alloc.free(analyzer);
        self.* = undefined;
    }
};

const ParsedTypeSpec = struct {
    field_type: ?[]const u8 = null,
    integer_only: bool = false,
    allows_null: bool = false,
};

const SchemaContext = struct {
    document_root: std.json.ObjectMap,
    scope_schema: std.json.ObjectMap,

    fn child(self: SchemaContext, object: std.json.ObjectMap) SchemaContext {
        return .{
            .document_root = self.document_root,
            .scope_schema = if (object.get("$defs") != null) object else self.scope_schema,
        };
    }
};

const RuntimeValidationContext = struct {
    alloc: std.mem.Allocator,
    root_property: ?*const DocumentProperty = null,
    active_root_ref_values: std.ArrayListUnmanaged(usize) = .{ .items = &.{}, .capacity = 0 },

    fn deinit(self: *RuntimeValidationContext) void {
        self.active_root_ref_values.deinit(self.alloc);
        self.* = undefined;
    }

    fn rootRefGuard(self: *RuntimeValidationContext, value: *const std.json.Value) !?RootRefGuard {
        const root_property = self.root_property orelse return error.InvalidBatchRequest;
        _ = root_property;
        const value_addr = @intFromPtr(value);
        for (self.active_root_ref_values.items) |active| {
            if (active == value_addr) return null;
        }
        try self.active_root_ref_values.append(self.alloc, value_addr);
        return .{ .ctx = self };
    }
};

const RootRefGuard = struct {
    ctx: *RuntimeValidationContext,

    fn release(self: RootRefGuard) void {
        _ = self.ctx.active_root_ref_values.pop();
    }
};

pub fn parseSchemaUpdateRequest(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    if (body.len == 0) return error.InvalidSchemaUpdateRequest;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    try validateSchemaValue(parsed.value);
    var schema = try parseTableSchemaValue(alloc, parsed.value);
    defer schema.deinit(alloc);
    try validateParsedTtlSchema(schema);
    try validateParsedDocumentJoinCardinalityProofs(schema);
    try validateParsedRelationalSchema(schema);
    return try stringifyJsonValue(alloc, parsed.value);
}

pub fn parseSchema(alloc: std.mem.Allocator, schema_json: []const u8) !TableSchema {
    if (schema_json.len == 0) {
        return .{
            .default_type = try alloc.dupe(u8, ""),
            .ttl_field = try alloc.dupe(u8, "_timestamp"),
        };
    }

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();
    try validateSchemaValue(parsed.value);
    const schema = try parseTableSchemaValue(alloc, parsed.value);
    errdefer {
        var owned = schema;
        owned.deinit(alloc);
    }
    try validateParsedTtlSchema(schema);
    try validateParsedDocumentJoinCardinalityProofs(schema);
    try validateParsedRelationalSchema(schema);
    return schema;
}

pub fn validateJsonSchemaJson(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    value_json: []const u8,
) !void {
    var schema_parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer schema_parsed.deinit();
    var value_parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
    defer value_parsed.deinit();
    try validateJsonSchemaValue(alloc, schema_parsed.value, value_parsed.value);
}

pub fn validateJsonSchemaValue(
    alloc: std.mem.Allocator,
    schema: std.json.Value,
    value: std.json.Value,
) !void {
    const schema_object = switch (schema) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    const context: SchemaContext = .{
        .document_root = schema_object,
        .scope_schema = schema_object,
    };
    const root_property = try parseAnonymousProperty(alloc, context, schema_object);
    defer {
        root_property.deinit(alloc);
        alloc.destroy(root_property);
    }

    var validation_context = RuntimeValidationContext{
        .alloc = alloc,
        .root_property = root_property,
    };
    defer validation_context.deinit();

    try validateDocumentFieldValueWithContext(&validation_context, root_property.*, &value, false);
}

pub fn documentTtlTimestampNs(
    alloc: std.mem.Allocator,
    schema: TableSchema,
    value_json: []const u8,
) !?u64 {
    if (schema.ttl_duration_ns == 0) return null;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidBatchRequest,
    };

    const ttl_value = root.get(schema.ttl_field) orelse return null;
    if (ttl_value == .null) return null;
    return try parseTtlTimestampNs(ttl_value);
}

pub fn validateWritesAgainstSchema(
    alloc: std.mem.Allocator,
    schema: TableSchema,
    writes: anytype,
) !void {
    const Writes = @TypeOf(writes);
    switch (@typeInfo(Writes)) {
        .pointer => |pointer| {
            if (pointer.size == .slice) {
                for (writes) |write| try validateDocumentJson(alloc, schema, write.value);
                return;
            }
            if (pointer.size == .one) {
                const child = @typeInfo(pointer.child);
                if (child == .array) {
                    for (writes.*) |write| try validateDocumentJson(alloc, schema, write.value);
                    return;
                }
                if (child == .@"struct" and child.@"struct".is_tuple) {
                    inline for (writes.*) |write| try validateDocumentJson(alloc, schema, write.value);
                    return;
                }
            }
        },
        .array => {
            for (writes) |write| try validateDocumentJson(alloc, schema, write.value);
            return;
        },
        .@"struct" => |struct_info| {
            if (struct_info.is_tuple) {
                inline for (writes) |write| try validateDocumentJson(alloc, schema, write.value);
                return;
            }
        },
        else => {},
    }
    @compileError("validateWritesAgainstSchema expects a slice, array, or tuple of writes");
}

pub fn validateDocumentJson(
    alloc: std.mem.Allocator,
    schema: TableSchema,
    value_json: []const u8,
) !void {
    if (schema.document_schemas.len == 0 and !schema.enforce_types and schema.ttl_duration_ns == 0 and schema.dynamic_templates.len == 0) return;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidBatchRequest,
    };

    const document_schema = try resolveDocumentSchema(schema, root);
    var validation_context = RuntimeValidationContext{ .alloc = alloc };
    defer validation_context.deinit();
    var root_property: ?DocumentProperty = null;
    var root_composition_evaluated_fields = std.StringHashMapUnmanaged(void).empty;
    defer root_composition_evaluated_fields.deinit(alloc);
    if (document_schema) |resolved_document_schema| {
        root_property = makeRootDocumentProperty(resolved_document_schema);
        validation_context.root_property = &root_property.?;
        try validateDocumentFieldValueWithContext(&validation_context, root_property.?, &parsed.value, false);
        try collectComposedObjectFieldCoverage(&validation_context, root_property.?, root, schema.enforce_types, &root_composition_evaluated_fields, false);
    }
    var it = root.iterator();
    while (it.next()) |entry| {
        const field_name = entry.key_ptr.*;
        if (schema.ttl_duration_ns > 0 and std.mem.eql(u8, field_name, schema.ttl_field)) {
            try validateTtlFieldValue(entry.value_ptr.*);
            continue;
        }
        if (shouldIgnoreSchemaValidationField(field_name)) continue;

        if (document_schema) |resolved_document_schema| {
            if (findDocumentProperty(resolved_document_schema.properties, field_name)) |property| {
                if (schema.storage_mode == .document and property.generated != null) return error.InvalidBatchRequest;
                try validateDocumentFieldValueWithContext(&validation_context, property, entry.value_ptr, schema.enforce_types);
                continue;
            }
            if (try validatePatternProperties(&validation_context, field_name, entry.value_ptr, resolved_document_schema.pattern_properties, schema.enforce_types)) {
                continue;
            }
        }
        if (fieldMatchesDynamicTemplates(schema.dynamic_templates, field_name, entry.value_ptr.*)) continue;
        if (document_schema) |resolved_document_schema| {
            if (resolved_document_schema.additional_properties_schema) |additional_properties_schema| {
                try validateDocumentFieldValueWithContext(&validation_context, additional_properties_schema.*, entry.value_ptr, schema.enforce_types);
                continue;
            }
            if (resolved_document_schema.additional_properties_allowed) |allowed| {
                if (!allowed) return error.InvalidBatchRequest;
                continue;
            }
        }
        if (root_composition_evaluated_fields.contains(field_name)) continue;
        if (root_property) |resolved_root_property| {
            if (resolved_root_property.unevaluated_properties_schema) |unevaluated_properties_schema| {
                try validateDocumentFieldValueWithContext(&validation_context, unevaluated_properties_schema.*, entry.value_ptr, schema.enforce_types);
                continue;
            }
            if (resolved_root_property.unevaluated_properties_allowed) |allowed| {
                if (!allowed) return error.InvalidBatchRequest;
                continue;
            }
        }
        if (schema.enforce_types) return error.InvalidBatchRequest;
    }
}

pub fn globMatch(pattern: []const u8, text: []const u8) bool {
    var pi: usize = 0;
    var ti: usize = 0;
    var star_pi: ?usize = null;
    var star_ti: usize = 0;

    while (ti < text.len) {
        if (pi < pattern.len and (pattern[pi] == text[ti] or pattern[pi] == '?')) {
            pi += 1;
            ti += 1;
        } else if (pi < pattern.len and pattern[pi] == '*') {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if (star_pi) |sp| {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while (pi < pattern.len and pattern[pi] == '*') pi += 1;
    return pi == pattern.len;
}

fn stringifyJsonValue(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

fn validateSchemaValue(value: std.json.Value) !void {
    const root = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    if (root.get("version")) |version| if (version != .null) try validateNonNegativeInteger(version);
    if (root.get("storage_mode")) |storage_mode| if (storage_mode != .null) switch (storage_mode) {
        .string => |text| {
            if (StorageMode.fromString(text) == null) return error.InvalidSchemaUpdateRequest;
        },
        else => return error.InvalidSchemaUpdateRequest,
    };
    if (root.get("default_type")) |default_type| if (default_type != .null and default_type != .string) return error.InvalidSchemaUpdateRequest;
    if (root.get("ttl_duration_ns")) |ttl_duration_ns| if (ttl_duration_ns != .null) try validateNonNegativeInteger(ttl_duration_ns);
    if (root.get("ttl_field")) |ttl_field| if (ttl_field != .null) switch (ttl_field) {
        .string => |text| {
            if (text.len == 0) return error.InvalidSchemaUpdateRequest;
        },
        else => return error.InvalidSchemaUpdateRequest,
    };
    if (root.get("enforce_types")) |enforce_types| if (enforce_types != .null and enforce_types != .bool) return error.InvalidSchemaUpdateRequest;
    if (root.get("document_schemas")) |document_schemas| if (document_schemas != .null) try validateDocumentSchemas(document_schemas);
    if (root.get("dynamic_templates")) |dynamic_templates| if (dynamic_templates != .null) try validateDynamicTemplates(dynamic_templates);
    if (root.get("primary_key")) |primary_key| if (primary_key != .null) try validatePrimaryKey(primary_key);
    if (root.get("periods")) |periods| if (periods != .null) try validateRelationalPeriodsValue(periods);
    if (root.get("foreign_keys")) |foreign_keys| if (foreign_keys != .null) try validateForeignKeys(foreign_keys);
    if (root.get("unique_constraints")) |constraints| if (constraints != .null) try validateUniqueConstraints(constraints);
    if (root.get("relational_indexes")) |indexes| if (indexes != .null) try validateRelationalIndexes(indexes);
    if (root.get("checks")) |checks| if (checks != .null) try validateRelationalChecksValue(checks);
    if (root.get("system_versioned")) |system_versioned| if (system_versioned != .null and system_versioned != .bool) return error.InvalidSchemaUpdateRequest;
    if (root.get("base_source") != null and root.get("external_base_source") != null) return error.InvalidSchemaUpdateRequest;
    if (root.get("base_source")) |base_source| if (base_source != .null) try validateExternalBaseSource(base_source);
    if (root.get("external_base_source")) |base_source| if (base_source != .null) try validateExternalBaseSource(base_source);
}

fn validateExternalBaseSource(value: std.json.Value) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    if (object.get("kind")) |kind| switch (kind) {
        .string => |text| if (!enumTokenEql(text, "external")) return error.InvalidSchemaUpdateRequest,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const table_id = object.get("table_id") orelse return error.InvalidSchemaUpdateRequest;
    if (table_id != .string or table_id.string.len == 0) return error.InvalidSchemaUpdateRequest;
    const format = object.get("format") orelse return error.InvalidSchemaUpdateRequest;
    if (format != .string or parseExternalBaseFormat(format.string) == null) return error.InvalidSchemaUpdateRequest;
    const uri = object.get("uri") orelse object.get("source_uri") orelse return error.InvalidSchemaUpdateRequest;
    if (uri != .string or uri.string.len == 0) return error.InvalidSchemaUpdateRequest;
    if (object.get("credentials")) |credentials| try validateExternalCredentialRef(credentials);
    if (object.get("credential_ref")) |credentials| try validateExternalCredentialRef(credentials);
    if (object.get("snapshot")) |snapshot| try validateExternalSnapshotMode(snapshot);
    if (object.get("snapshot_mode")) |snapshot| try validateExternalSnapshotMode(snapshot);
    const schema_fingerprint = object.get("schema_fingerprint") orelse return error.InvalidSchemaUpdateRequest;
    if (schema_fingerprint != .string or schema_fingerprint.string.len == 0) return error.InvalidSchemaUpdateRequest;
    if (object.get("write_policy")) |write_policy| {
        if (write_policy != .string or parseExternalWritePolicy(write_policy.string) == null) return error.InvalidSchemaUpdateRequest;
    }
}

fn validateExternalCredentialRef(value: std.json.Value) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const ref_id = object.get("ref") orelse object.get("ref_id") orelse return error.InvalidSchemaUpdateRequest;
    if (ref_id != .string or ref_id.string.len == 0) return error.InvalidSchemaUpdateRequest;
    if (object.get("scope")) |scope| {
        if (scope != .string) return error.InvalidSchemaUpdateRequest;
    }
}

fn validateExternalSnapshotMode(value: std.json.Value) !void {
    switch (value) {
        .string => |text| {
            if (!enumTokenEql(text, "current") and !enumTokenEql(text, "iceberg_current")) return error.InvalidSchemaUpdateRequest;
        },
        .object => |object| {
            const mode = object.get("mode") orelse return error.InvalidSchemaUpdateRequest;
            if (mode != .string) return error.InvalidSchemaUpdateRequest;
            if (enumTokenEql(mode.string, "current") or enumTokenEql(mode.string, "iceberg_current")) return;
            if (enumTokenEql(mode.string, "snapshot_id") or enumTokenEql(mode.string, "snapshot")) {
                const id = object.get("id") orelse object.get("snapshot_id") orelse return error.InvalidSchemaUpdateRequest;
                if (id != .string or id.string.len == 0) return error.InvalidSchemaUpdateRequest;
                return;
            }
            if (enumTokenEql(mode.string, "object_version_digest") or enumTokenEql(mode.string, "raw_parquet_digest")) {
                const digest = object.get("digest") orelse object.get("object_version_digest") orelse return error.InvalidSchemaUpdateRequest;
                if (digest != .string or digest.string.len == 0) return error.InvalidSchemaUpdateRequest;
                return;
            }
            return error.InvalidSchemaUpdateRequest;
        },
        else => return error.InvalidSchemaUpdateRequest,
    }
}

fn validatePrimaryKey(value: std.json.Value) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    var field_it = object.iterator();
    while (field_it.next()) |entry| {
        if (!std.mem.eql(u8, entry.key_ptr.*, "name") and
            !std.mem.eql(u8, entry.key_ptr.*, "columns") and
            !std.mem.eql(u8, entry.key_ptr.*, "include_columns") and
            !std.mem.eql(u8, entry.key_ptr.*, "without_overlaps_period") and
            !std.mem.eql(u8, entry.key_ptr.*, "timing") and
            !std.mem.eql(u8, entry.key_ptr.*, "deferrable"))
        {
            return error.InvalidSchemaUpdateRequest;
        }
    }
    if (object.get("name")) |name| {
        if (name != .string or name.string.len == 0) return error.InvalidSchemaUpdateRequest;
    }
    const columns = object.get("columns") orelse return error.InvalidSchemaUpdateRequest;
    try validateStringArray(columns, true);
    if (object.get("include_columns")) |include_columns| try validateStringArray(include_columns, false);
    if (object.get("without_overlaps_period")) |period| {
        if (period != .string or period.string.len == 0) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("timing")) |timing| {
        if (timing != .string or foreignKeyTimingClauseFromString(timing.string) == null) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("deferrable")) |deferrable| {
        if (foreignKeyDeferrabilityFromValue(deferrable) == null) return error.InvalidSchemaUpdateRequest;
    }
    _ = try constraintTimingMetadataFromObject(object);
}

fn validateRelationalPeriodsValue(value: std.json.Value) !void {
    const array = switch (value) {
        .array => |array| array,
        else => return error.InvalidSchemaUpdateRequest,
    };
    for (array.items) |item| {
        const object = switch (item) {
            .object => |object| object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        var it = object.iterator();
        while (it.next()) |entry| {
            if (!std.mem.eql(u8, entry.key_ptr.*, "name") and
                !std.mem.eql(u8, entry.key_ptr.*, "start_column") and
                !std.mem.eql(u8, entry.key_ptr.*, "end_column") and
                !std.mem.eql(u8, entry.key_ptr.*, "range_type"))
            {
                return error.InvalidSchemaUpdateRequest;
            }
        }
        const name = object.get("name") orelse return error.InvalidSchemaUpdateRequest;
        const start_column = object.get("start_column") orelse return error.InvalidSchemaUpdateRequest;
        const end_column = object.get("end_column") orelse return error.InvalidSchemaUpdateRequest;
        if (name != .string or name.string.len == 0) return error.InvalidSchemaUpdateRequest;
        if (start_column != .string or start_column.string.len == 0) return error.InvalidSchemaUpdateRequest;
        if (end_column != .string or end_column.string.len == 0) return error.InvalidSchemaUpdateRequest;
        if (std.mem.eql(u8, start_column.string, end_column.string)) return error.InvalidSchemaUpdateRequest;
        if (object.get("range_type")) |range_type| {
            if (range_type != .string) return error.InvalidSchemaUpdateRequest;
            _ = parseRelationalPeriodRangeType(range_type.string) orelse return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn validateForeignKeys(value: std.json.Value) !void {
    const array = switch (value) {
        .array => |array| array,
        else => return error.InvalidSchemaUpdateRequest,
    };
    for (array.items) |item| {
        const object = switch (item) {
            .object => |object| object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        var field_it = object.iterator();
        while (field_it.next()) |entry| {
            if (!isAllowedForeignKeyField(entry.key_ptr.*)) return error.InvalidSchemaUpdateRequest;
        }
        const name = object.get("name") orelse return error.InvalidSchemaUpdateRequest;
        if (name != .string or name.string.len == 0) return error.InvalidSchemaUpdateRequest;
        const columns = object.get("columns") orelse return error.InvalidSchemaUpdateRequest;
        try validateStringArray(columns, true);
        const references = object.get("references") orelse return error.InvalidSchemaUpdateRequest;
        const references_object = switch (references) {
            .object => |references_object| references_object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        var references_field_it = references_object.iterator();
        while (references_field_it.next()) |entry| {
            if (!isAllowedForeignKeyReferenceField(entry.key_ptr.*)) return error.InvalidSchemaUpdateRequest;
        }
        const parent_table = references_object.get("table") orelse return error.InvalidSchemaUpdateRequest;
        if (parent_table != .string or parent_table.string.len == 0) return error.InvalidSchemaUpdateRequest;
        const parent_columns = references_object.get("columns") orelse return error.InvalidSchemaUpdateRequest;
        try validateStringArray(parent_columns, true);
        if (object.get("period")) |period| {
            if (period != .string or period.string.len == 0) return error.InvalidSchemaUpdateRequest;
        }
        if (references_object.get("period")) |period| {
            if (period != .string or period.string.len == 0) return error.InvalidSchemaUpdateRequest;
        }
        if (object.get("on_delete")) |on_delete| {
            if (on_delete != .string or ForeignKeyAction.fromString(on_delete.string) == null) return error.InvalidSchemaUpdateRequest;
        }
        if (object.get("on_update")) |on_update| {
            if (on_update != .string or ForeignKeyAction.fromString(on_update.string) == null) return error.InvalidSchemaUpdateRequest;
        }
        if (object.get("timing")) |timing| {
            if (timing != .string or foreignKeyTimingClauseFromString(timing.string) == null) return error.InvalidSchemaUpdateRequest;
        }
        if (object.get("deferrable")) |deferrable| {
            if (foreignKeyDeferrableFromValue(deferrable) == null) return error.InvalidSchemaUpdateRequest;
        }
        if (object.get("match")) |match| {
            if (match != .string or ForeignKeyMatch.fromString(match.string) == null) return error.InvalidSchemaUpdateRequest;
        }
        if (object.get("validation_state")) |validation_state| {
            if (validation_state != .string or ForeignKeyValidationState.fromString(validation_state.string) == null) return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn isAllowedForeignKeyField(field: []const u8) bool {
    return std.mem.eql(u8, field, "name") or
        std.mem.eql(u8, field, "columns") or
        std.mem.eql(u8, field, "period") or
        std.mem.eql(u8, field, "references") or
        std.mem.eql(u8, field, "on_delete") or
        std.mem.eql(u8, field, "on_update") or
        std.mem.eql(u8, field, "timing") or
        std.mem.eql(u8, field, "deferrable") or
        std.mem.eql(u8, field, "match") or
        std.mem.eql(u8, field, "validation_state");
}

fn isAllowedForeignKeyReferenceField(field: []const u8) bool {
    return std.mem.eql(u8, field, "table") or
        std.mem.eql(u8, field, "columns") or
        std.mem.eql(u8, field, "period");
}

fn validateUniqueConstraints(value: std.json.Value) !void {
    const array = switch (value) {
        .array => |array| array,
        else => return error.InvalidSchemaUpdateRequest,
    };
    for (array.items) |item| {
        const object = switch (item) {
            .object => |object| object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        var field_it = object.iterator();
        while (field_it.next()) |entry| {
            if (!isAllowedUniqueConstraintField(entry.key_ptr.*)) return error.InvalidSchemaUpdateRequest;
        }
        const name = object.get("name") orelse return error.InvalidSchemaUpdateRequest;
        if (name != .string or name.string.len == 0) return error.InvalidSchemaUpdateRequest;
        const columns = object.get("columns");
        const expressions = object.get("expressions");
        if (columns == null and expressions == null) return error.InvalidSchemaUpdateRequest;
        if (columns) |columns_value| try validateStringArray(columns_value, false);
        if (expressions) |expressions_value| try validateUniqueExpressionArray(expressions_value);
        if (object.get("include_columns")) |include_columns| try validateStringArray(include_columns, false);
        const column_count = if (columns) |columns_value| columns_value.array.items.len else 0;
        const expression_count = if (expressions) |expressions_value| expressions_value.array.items.len else 0;
        if (column_count + expression_count == 0) return error.InvalidSchemaUpdateRequest;
        if (object.get("without_overlaps_period")) |period| {
            if (period != .string or period.string.len == 0) return error.InvalidSchemaUpdateRequest;
        }
        if (object.get("nulls_not_distinct")) |nulls_not_distinct| {
            if (nulls_not_distinct != .bool) return error.InvalidSchemaUpdateRequest;
        }
        if (object.get("where")) |where| {
            try validateUniquePredicateDefinition(where);
        }
        if (object.get("where_expressions")) |where_expressions| {
            try validateRelationalRowsExpressionConditionArrayJson(where_expressions);
        }
        if (object.get("validation_state")) |validation_state| {
            if (validation_state != .string or UniqueConstraintValidationState.fromString(validation_state.string) == null) return error.InvalidSchemaUpdateRequest;
        }
        if (object.get("timing")) |timing| {
            if (timing != .string or foreignKeyTimingClauseFromString(timing.string) == null) return error.InvalidSchemaUpdateRequest;
        }
        if (object.get("deferrable")) |deferrable| {
            if (foreignKeyDeferrabilityFromValue(deferrable) == null) return error.InvalidSchemaUpdateRequest;
        }
        _ = try constraintTimingMetadataFromObject(object);
    }
}

fn isAllowedUniqueConstraintField(field: []const u8) bool {
    return std.mem.eql(u8, field, "name") or
        std.mem.eql(u8, field, "columns") or
        std.mem.eql(u8, field, "expressions") or
        std.mem.eql(u8, field, "include_columns") or
        std.mem.eql(u8, field, "without_overlaps_period") or
        std.mem.eql(u8, field, "nulls_not_distinct") or
        std.mem.eql(u8, field, "timing") or
        std.mem.eql(u8, field, "deferrable") or
        std.mem.eql(u8, field, "where") or
        std.mem.eql(u8, field, "where_expressions") or
        std.mem.eql(u8, field, "validation_state");
}

fn validateRelationalIndexes(value: std.json.Value) !void {
    const array = switch (value) {
        .array => |array| array,
        else => return error.InvalidSchemaUpdateRequest,
    };
    for (array.items) |item| {
        const object = switch (item) {
            .object => |object| object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        var field_it = object.iterator();
        while (field_it.next()) |entry| {
            if (!isAllowedRelationalIndexField(entry.key_ptr.*)) return error.InvalidSchemaUpdateRequest;
        }
        const name = object.get("name") orelse return error.InvalidSchemaUpdateRequest;
        if (name != .string or name.string.len == 0) return error.InvalidSchemaUpdateRequest;
        const owner_kind = object.get("owner_kind") orelse return error.InvalidSchemaUpdateRequest;
        if (owner_kind != .string or relationalIndexOwnerKindFromString(owner_kind.string) == null) return error.InvalidSchemaUpdateRequest;
        const owner_name = object.get("owner_name") orelse return error.InvalidSchemaUpdateRequest;
        if (owner_name != .string or owner_name.string.len == 0) return error.InvalidSchemaUpdateRequest;
        const access_method = object.get("access_method") orelse return error.InvalidSchemaUpdateRequest;
        const parsed_access_method = if (access_method == .string) storage_schema.RelationalIndexAccessMethod.fromString(access_method.string) else null;
        if (parsed_access_method == null) return error.InvalidSchemaUpdateRequest;
        if (object.get("method_config")) |method_config| try validateRelationalIndexMethodConfig(method_config, parsed_access_method.?);
        if (object.get("unique")) |unique| {
            if (unique != .bool) return error.InvalidSchemaUpdateRequest;
        }
        if (object.get("columns")) |columns| try validateStringArray(columns, false);
        try validateRelationalIndexMethodConfigCatalog(object, parsed_access_method.?);
        if (object.get("expressions")) |expressions| try validateUniqueExpressionArray(expressions);
        if (object.get("include_columns")) |include_columns| try validateStringArray(include_columns, false);
        if (object.get("keys")) |keys| try validateRelationalIndexKeysJson(keys);
        if (object.get("lifecycle")) |lifecycle| {
            if (lifecycle != .string or storageRelationalIndexLifecycleFromString(lifecycle.string) == null) return error.InvalidSchemaUpdateRequest;
        }
        if (object.get("generation")) |generation| {
            if (generation != .integer or generation.integer <= 0) return error.InvalidSchemaUpdateRequest;
        }
        if (object.get("schema_fingerprint")) |fingerprint| {
            if (fingerprint != .string or fingerprint.string.len == 0) return error.InvalidSchemaUpdateRequest;
        }
        if (object.get("where")) |where| try validateUniquePredicateDefinition(where);
        if (object.get("where_expressions")) |where_expressions| try validateRelationalRowsExpressionConditionArrayJson(where_expressions);
    }
}

fn isAllowedRelationalIndexField(field: []const u8) bool {
    return std.mem.eql(u8, field, "name") or
        std.mem.eql(u8, field, "owner_kind") or
        std.mem.eql(u8, field, "owner_name") or
        std.mem.eql(u8, field, "access_method") or
        std.mem.eql(u8, field, "method_config") or
        std.mem.eql(u8, field, "unique") or
        std.mem.eql(u8, field, "columns") or
        std.mem.eql(u8, field, "expressions") or
        std.mem.eql(u8, field, "include_columns") or
        std.mem.eql(u8, field, "keys") or
        std.mem.eql(u8, field, "lifecycle") or
        std.mem.eql(u8, field, "generation") or
        std.mem.eql(u8, field, "schema_fingerprint") or
        std.mem.eql(u8, field, "where") or
        std.mem.eql(u8, field, "where_expressions");
}

fn validateRelationalIndexMethodConfig(value: std.json.Value, access_method: storage_schema.RelationalIndexAccessMethod) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    switch (access_method) {
        .scalar_column, .ordered_tuple => return error.InvalidSchemaUpdateRequest,
        .text_search => {
            const type_value = object.get("type") orelse return error.InvalidSchemaUpdateRequest;
            if (type_value != .string or !std.mem.eql(u8, type_value.string, "full_text")) return error.InvalidSchemaUpdateRequest;
            const field = object.get("field") orelse return error.InvalidSchemaUpdateRequest;
            if (field != .string or field.string.len == 0) return error.InvalidSchemaUpdateRequest;
            var it = object.iterator();
            while (it.next()) |entry| {
                const key = entry.key_ptr.*;
                const item = entry.value_ptr.*;
                if (std.mem.eql(u8, key, "type") or std.mem.eql(u8, key, "field")) continue;
                if (std.mem.eql(u8, key, "analyzer") or
                    std.mem.eql(u8, key, "scoring") or
                    std.mem.eql(u8, key, "segment_lifecycle"))
                {
                    const string_value = switch (item) {
                        .string => |string| string,
                        else => return error.InvalidSchemaUpdateRequest,
                    };
                    if (string_value.len == 0) return error.InvalidSchemaUpdateRequest;
                } else if (std.mem.eql(u8, key, "highlight") or std.mem.eql(u8, key, "snippet")) {
                    if (item != .bool) return error.InvalidSchemaUpdateRequest;
                } else return error.InvalidSchemaUpdateRequest;
            }
        },
        .algebraic_filter => {
            const type_value = object.get("type") orelse return error.InvalidSchemaUpdateRequest;
            if (type_value != .string or !std.mem.eql(u8, type_value.string, "algebraic")) return error.InvalidSchemaUpdateRequest;
            const derive = object.get("derive_from_schema") orelse return error.InvalidSchemaUpdateRequest;
            if (derive != .bool or !derive.bool) return error.InvalidSchemaUpdateRequest;
            var it = object.iterator();
            while (it.next()) |entry| {
                const key = entry.key_ptr.*;
                if (!std.mem.eql(u8, key, "type") and !std.mem.eql(u8, key, "derive_from_schema")) return error.InvalidSchemaUpdateRequest;
            }
        },
    }
}

fn validateRelationalIndexMethodConfigCatalog(
    object: std.json.ObjectMap,
    access_method: storage_schema.RelationalIndexAccessMethod,
) !void {
    const owner_kind_value = object.get("owner_kind") orelse return error.InvalidSchemaUpdateRequest;
    const owner_kind = relationalIndexOwnerKindFromString(owner_kind_value.string) orelse return error.InvalidSchemaUpdateRequest;
    const owner_name_value = object.get("owner_name") orelse return error.InvalidSchemaUpdateRequest;
    if (owner_name_value != .string) return error.InvalidSchemaUpdateRequest;
    const owner_name = owner_name_value.string;
    const method_config = object.get("method_config");

    switch (access_method) {
        .scalar_column, .ordered_tuple => if (method_config != null) return error.InvalidSchemaUpdateRequest,
        .text_search => {
            const config = method_config orelse return error.InvalidSchemaUpdateRequest;
            const config_object = switch (config) {
                .object => |config_object| config_object,
                else => return error.InvalidSchemaUpdateRequest,
            };
            if (owner_kind != .relational_column) return error.InvalidSchemaUpdateRequest;
            const columns_value = object.get("columns") orelse return error.InvalidSchemaUpdateRequest;
            const columns = switch (columns_value) {
                .array => |columns| columns,
                else => return error.InvalidSchemaUpdateRequest,
            };
            if (columns.items.len != 1) return error.InvalidSchemaUpdateRequest;
            const column = switch (columns.items[0]) {
                .string => |column| column,
                else => return error.InvalidSchemaUpdateRequest,
            };
            if (!std.mem.eql(u8, column, owner_name)) return error.InvalidSchemaUpdateRequest;
            const field_value = config_object.get("field") orelse return error.InvalidSchemaUpdateRequest;
            if (field_value != .string or !std.mem.eql(u8, field_value.string, owner_name)) return error.InvalidSchemaUpdateRequest;
        },
        .algebraic_filter => if (method_config != null) {
            if (owner_kind != .table) return error.InvalidSchemaUpdateRequest;
            if (!std.mem.eql(u8, owner_name, storage_schema.relational_table_index_owner_name)) return error.InvalidSchemaUpdateRequest;
            if (object.get("columns")) |columns_value| {
                const columns = switch (columns_value) {
                    .array => |columns| columns,
                    else => return error.InvalidSchemaUpdateRequest,
                };
                if (columns.items.len != 0) return error.InvalidSchemaUpdateRequest;
            }
        },
    }
}

fn relationalIndexOwnerKindFromString(value: []const u8) ?storage_schema.RelationalIndexOwnerKind {
    if (std.mem.eql(u8, value, "relational_column")) return .relational_column;
    if (std.mem.eql(u8, value, "unique_constraint")) return .unique_constraint;
    if (std.mem.eql(u8, value, "table")) return .table;
    return null;
}

fn validateStringArray(value: std.json.Value, require_non_empty: bool) !void {
    const array = switch (value) {
        .array => |array| array,
        else => return error.InvalidSchemaUpdateRequest,
    };
    if (require_non_empty and array.items.len == 0) return error.InvalidSchemaUpdateRequest;
    for (array.items) |item| {
        if (item != .string or item.string.len == 0) return error.InvalidSchemaUpdateRequest;
    }
}

fn validateRelationalIndexKeysJson(value: std.json.Value) !void {
    const array = switch (value) {
        .array => |array| array,
        else => return error.InvalidSchemaUpdateRequest,
    };
    if (array.items.len == 0) return error.InvalidSchemaUpdateRequest;
    for (array.items) |item| {
        const object = switch (item) {
            .object => |object| object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        var it = object.iterator();
        while (it.next()) |entry| {
            if (!std.mem.eql(u8, entry.key_ptr.*, "column") and
                !std.mem.eql(u8, entry.key_ptr.*, "direction") and
                !std.mem.eql(u8, entry.key_ptr.*, "nulls"))
            {
                return error.InvalidSchemaUpdateRequest;
            }
        }
        const column = object.get("column") orelse return error.InvalidSchemaUpdateRequest;
        if (column != .string or column.string.len == 0) return error.InvalidSchemaUpdateRequest;
        if (object.get("direction")) |direction| {
            if (direction != .string or relationalIndexKeyDirectionFromString(direction.string) == null) return error.InvalidSchemaUpdateRequest;
        }
        if (object.get("nulls")) |nulls| {
            if (nulls != .string or relationalIndexKeyNullsFromString(nulls.string) == null) return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn validateUniqueExpressionArray(value: std.json.Value) !void {
    const array = switch (value) {
        .array => |array| array,
        else => return error.InvalidSchemaUpdateRequest,
    };
    for (array.items) |item| {
        const object = switch (item) {
            .object => |object| object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        var it = object.iterator();
        while (it.next()) |entry| {
            if (!std.mem.eql(u8, entry.key_ptr.*, "op") and
                !std.mem.eql(u8, entry.key_ptr.*, "field") and
                !std.mem.eql(u8, entry.key_ptr.*, "expression"))
            {
                return error.InvalidSchemaUpdateRequest;
            }
        }
        const op = object.get("op") orelse return error.InvalidSchemaUpdateRequest;
        if (op != .string) return error.InvalidSchemaUpdateRequest;
        if (enumTokenEql(op.string, "expression")) {
            if (object.get("field") != null) return error.InvalidSchemaUpdateRequest;
            const expression = object.get("expression") orelse return error.InvalidSchemaUpdateRequest;
            try validateRelationalRowsExpressionJson(expression);
        } else {
            if (!enumTokenEql(op.string, "lower") and !enumTokenEql(op.string, "upper") and !enumTokenEql(op.string, "md5")) return error.InvalidSchemaUpdateRequest;
            if (object.get("expression") != null) return error.InvalidSchemaUpdateRequest;
            const field = object.get("field") orelse return error.InvalidSchemaUpdateRequest;
            if (field != .string or field.string.len == 0) return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn validateUniquePredicateDefinition(value: std.json.Value) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    var it = object.iterator();
    while (it.next()) |entry| {
        if (!std.mem.eql(u8, entry.key_ptr.*, "all")) return error.InvalidSchemaUpdateRequest;
    }
    const all = object.get("all") orelse return error.InvalidSchemaUpdateRequest;
    const array = switch (all) {
        .array => |array| array,
        else => return error.InvalidSchemaUpdateRequest,
    };
    if (array.items.len == 0) return error.InvalidSchemaUpdateRequest;
    for (array.items) |item| try validateUniquePredicateAtom(item);
}

fn validateUniquePredicateAtom(value: std.json.Value) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    var it = object.iterator();
    while (it.next()) |entry| {
        if (!std.mem.eql(u8, entry.key_ptr.*, "field") and
            !std.mem.eql(u8, entry.key_ptr.*, "op") and
            !std.mem.eql(u8, entry.key_ptr.*, "value"))
        {
            return error.InvalidSchemaUpdateRequest;
        }
    }
    const field = object.get("field") orelse return error.InvalidSchemaUpdateRequest;
    const op = object.get("op") orelse return error.InvalidSchemaUpdateRequest;
    if (field != .string or field.string.len == 0) return error.InvalidSchemaUpdateRequest;
    if (op != .string) return error.InvalidSchemaUpdateRequest;
    const needs_value = enumTokenEql(op.string, "eq") or enumTokenEql(op.string, "ne");
    const forbids_value = enumTokenEql(op.string, "is_null") or enumTokenEql(op.string, "is_not_null");
    if (!needs_value and !forbids_value) return error.InvalidSchemaUpdateRequest;
    if (needs_value and object.get("value") == null) return error.InvalidSchemaUpdateRequest;
    if (forbids_value and object.get("value") != null) return error.InvalidSchemaUpdateRequest;
}

fn validateRelationalChecksValue(value: std.json.Value) !void {
    const array = switch (value) {
        .array => |array| array,
        else => return error.InvalidSchemaUpdateRequest,
    };
    for (array.items) |item| {
        const object = switch (item) {
            .object => |object| object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        var it = object.iterator();
        while (it.next()) |entry| {
            if (!std.mem.eql(u8, entry.key_ptr.*, "name") and
                !std.mem.eql(u8, entry.key_ptr.*, "field") and
                !std.mem.eql(u8, entry.key_ptr.*, "op") and
                !std.mem.eql(u8, entry.key_ptr.*, "value") and
                !std.mem.eql(u8, entry.key_ptr.*, "collation") and
                !std.mem.eql(u8, entry.key_ptr.*, "expression") and
                !std.mem.eql(u8, entry.key_ptr.*, "validation_state"))
            {
                return error.InvalidSchemaUpdateRequest;
            }
        }
        const name = object.get("name") orelse return error.InvalidSchemaUpdateRequest;
        if (name != .string or name.string.len == 0) return error.InvalidSchemaUpdateRequest;
        if (object.get("expression")) |expression| {
            if (object.get("field") != null or object.get("op") != null or object.get("value") != null or object.get("collation") != null) return error.InvalidSchemaUpdateRequest;
            try validateRelationalRowsExpressionConditionJson(expression);
        } else {
            const field = object.get("field") orelse return error.InvalidSchemaUpdateRequest;
            const op = object.get("op") orelse return error.InvalidSchemaUpdateRequest;
            if (field != .string or field.string.len == 0) return error.InvalidSchemaUpdateRequest;
            if (op != .string) return error.InvalidSchemaUpdateRequest;
            const needs_value =
                enumTokenEql(op.string, "eq") or enumTokenEql(op.string, "ne") or
                enumTokenEql(op.string, "gt") or enumTokenEql(op.string, "gte") or
                enumTokenEql(op.string, "lt") or enumTokenEql(op.string, "lte") or
                enumTokenEql(op.string, "is_distinct") or enumTokenEql(op.string, "is_not_distinct");
            const forbids_value = enumTokenEql(op.string, "is_null") or enumTokenEql(op.string, "is_not_null");
            if (!needs_value and !forbids_value) return error.InvalidSchemaUpdateRequest;
            if (needs_value and object.get("value") == null) return error.InvalidSchemaUpdateRequest;
            if (forbids_value and object.get("value") != null) return error.InvalidSchemaUpdateRequest;
            if (object.get("collation")) |collation| {
                if (collation != .string or collation.string.len == 0) return error.InvalidSchemaUpdateRequest;
            }
        }
        if (object.get("validation_state")) |validation_state| {
            if (validation_state != .string) return error.InvalidSchemaUpdateRequest;
            _ = try parseRelationalCheckValidationState(validation_state.string);
        }
    }
}

fn validateDocumentSchemas(value: std.json.Value) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var it = object.iterator();
    while (it.next()) |entry| {
        const doc_schema = switch (entry.value_ptr.*) {
            .object => |doc_schema| doc_schema,
            else => return error.InvalidSchemaUpdateRequest,
        };
        const schema_value = doc_schema.get("schema") orelse return error.InvalidSchemaUpdateRequest;
        try validateDocumentSchemaDefinition(schema_value);
    }
}

fn validateDocumentSchemaDefinition(value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    return validateDocumentSchemaDefinitionWithContext(.{
        .document_root = object,
        .scope_schema = object,
    }, value);
}

fn validateDocumentSchemaDefinitionWithContext(context: SchemaContext, value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const current_context = context.child(object);
    if (object.get("$ref")) |ref_value| {
        const ref_path = try parseSchemaRefPath(ref_value);
        if (!isRootSchemaRef(ref_path)) {
            try validateDocumentSchemaDefinitionWithContext(current_context, .{
                .object = try resolveSchemaRef(current_context, ref_path),
            });
        }
    }
    try validateDocumentSchemaKeywords(current_context, object);
}

fn validateDocumentSchemaKeywords(context: SchemaContext, object: std.json.ObjectMap) anyerror!void {
    if (object.get("type")) |schema_type| {
        if (schema_type != .null) _ = try validateTypeSpecDefinition(schema_type, true);
    }
    if (object.get("format")) |format| {
        if (format != .null and format != .string) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("nullable")) |nullable| {
        if (nullable != .null and nullable != .bool) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("x-antfly-include-in-all")) |include_in_all| {
        if (include_in_all != .null) try validateAntflyIncludeInAllDefinition(include_in_all);
    }
    if (object.get("schema")) |embedded_schema| {
        if (embedded_schema != .null) try validateDocumentSchemaDefinition(embedded_schema);
    }
    if (object.get("dynamic_templates")) |dynamic_templates| {
        if (dynamic_templates != .null) try validateDynamicTemplates(dynamic_templates);
    }
    if (object.get("$defs")) |definitions| {
        if (definitions != .null) try validateDefinitionsDefinition(context, definitions);
    }
    if (object.get("properties")) |properties| {
        if (properties != .null) {
            if (properties != .object) return error.InvalidSchemaUpdateRequest;
            var it = properties.object.iterator();
            while (it.next()) |entry| try validatePropertySchemaDefinitionWithContext(context, entry.value_ptr.*);
        }
    }
    if (object.get("required")) |required| {
        if (required != .null) {
            if (required != .array) return error.InvalidSchemaUpdateRequest;
            for (required.array.items) |entry| {
                if (entry != .string) return error.InvalidSchemaUpdateRequest;
            }
        }
    }
    if (object.get("propertyNames")) |property_names| {
        if (property_names != .null) try validatePropertySchemaDefinitionWithContext(context, property_names);
    }
    if (object.get("patternProperties")) |pattern_properties| {
        if (pattern_properties != .null) try validatePatternPropertiesDefinition(context, pattern_properties);
    }
    if (object.get("additionalProperties")) |additional_properties| {
        if (additional_properties != .null and additional_properties != .bool and additional_properties != .object) {
            return error.InvalidSchemaUpdateRequest;
        }
        if (additional_properties == .object) try validatePropertySchemaDefinitionWithContext(context, additional_properties);
    }
    if (object.get("unevaluatedProperties")) |unevaluated_properties| {
        if (unevaluated_properties != .null and unevaluated_properties != .bool and unevaluated_properties != .object) {
            return error.InvalidSchemaUpdateRequest;
        }
        if (unevaluated_properties == .object) try validatePropertySchemaDefinitionWithContext(context, unevaluated_properties);
    }
    if (object.get("dependentRequired")) |dependent_required| {
        if (dependent_required != .null) try validateDependentRequiredDefinition(dependent_required);
    }
    if (object.get("dependentSchemas")) |dependent_schemas| {
        if (dependent_schemas != .null) try validateDependentSchemasDefinition(context, dependent_schemas);
    }
    if (object.get("dependencies")) |dependencies| {
        if (dependencies != .null) try validateDependenciesDefinition(context, dependencies);
    }
    if (object.get("items")) |items| {
        if (items != .null) try validatePropertySchemaDefinitionWithContext(context, items);
    }
    if (object.get("prefixItems")) |prefix_items| {
        if (prefix_items != .null) try validatePrefixItemsDefinition(context, prefix_items);
    }
    try validateAdditionalItemsDefinition(context, object);
    if (object.get("unevaluatedItems")) |unevaluated_items| {
        if (unevaluated_items != .null and unevaluated_items != .bool and unevaluated_items != .object) {
            return error.InvalidSchemaUpdateRequest;
        }
        if (unevaluated_items == .object) try validatePropertySchemaDefinitionWithContext(context, unevaluated_items);
    }
    if (object.get("contains")) |contains| {
        if (contains != .null) try validatePropertySchemaDefinitionWithContext(context, contains);
    }
    if (object.get("const")) |_| {}
    if (object.get("enum")) |enum_values| {
        if (enum_values != .null) try validateEnumDefinition(enum_values);
    }
    if (object.get("minimum")) |minimum| {
        if (minimum != .null) _ = parseJsonNumber(minimum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("maximum")) |maximum| {
        if (maximum != .null) _ = parseJsonNumber(maximum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("exclusiveMinimum")) |exclusive_minimum| {
        if (exclusive_minimum != .null) _ = parseJsonNumber(exclusive_minimum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("exclusiveMaximum")) |exclusive_maximum| {
        if (exclusive_maximum != .null) _ = parseJsonNumber(exclusive_maximum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("multipleOf")) |multiple_of| {
        if (multiple_of != .null) try validatePositiveNumber(multiple_of);
    }
    if (object.get("anyOf")) |any_of| {
        if (any_of != .null) try validateAnyOfDefinition(context, any_of);
    }
    if (object.get("oneOf")) |one_of| {
        if (one_of != .null) try validateOneOfDefinition(context, one_of);
    }
    if (object.get("allOf")) |all_of| {
        if (all_of != .null) try validateAllOfDefinition(context, all_of);
    }
    if (object.get("not")) |not_schema| {
        if (not_schema != .null) try validatePropertySchemaDefinitionWithContext(context, not_schema);
    }
    if (object.get("if")) |if_schema| {
        if (if_schema != .null) try validatePropertySchemaDefinitionWithContext(context, if_schema);
    }
    if (object.get("then")) |then_schema| {
        if (then_schema != .null) try validatePropertySchemaDefinitionWithContext(context, then_schema);
    }
    if (object.get("else")) |else_schema| {
        if (else_schema != .null) try validatePropertySchemaDefinitionWithContext(context, else_schema);
    }
    if ((object.get("then") != null or object.get("else") != null) and object.get("if") == null) {
        return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("pattern")) |pattern| {
        if (pattern != .null and pattern != .string) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("minLength")) |min_length| {
        if (min_length != .null) try validateNonNegativeInteger(min_length);
    }
    if (object.get("maxLength")) |max_length| {
        if (max_length != .null) try validateNonNegativeInteger(max_length);
    }
    if (object.get("minProperties")) |min_properties| {
        if (min_properties != .null) try validateNonNegativeInteger(min_properties);
    }
    if (object.get("maxProperties")) |max_properties| {
        if (max_properties != .null) try validateNonNegativeInteger(max_properties);
    }
    if (object.get("minItems")) |min_items| {
        if (min_items != .null) try validateNonNegativeInteger(min_items);
    }
    if (object.get("maxItems")) |max_items| {
        if (max_items != .null) try validateNonNegativeInteger(max_items);
    }
    if (object.get("minContains")) |min_contains| {
        if (min_contains != .null) try validateNonNegativeInteger(min_contains);
    }
    if (object.get("maxContains")) |max_contains| {
        if (max_contains != .null) try validateNonNegativeInteger(max_contains);
    }
    if ((object.get("minContains") != null or object.get("maxContains") != null) and object.get("contains") == null) {
        return error.InvalidSchemaUpdateRequest;
    }
}

fn validatePropertySchemaDefinition(value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    return validatePropertySchemaDefinitionWithContext(.{
        .document_root = object,
        .scope_schema = object,
    }, value);
}

fn validatePropertySchemaDefinitionWithContext(context: SchemaContext, value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const current_context = context.child(object);
    if (object.get("$ref")) |ref_value| {
        const ref_path = try parseSchemaRefPath(ref_value);
        if (!isRootSchemaRef(ref_path)) {
            try validatePropertySchemaDefinitionWithContext(current_context, .{
                .object = try resolveSchemaRef(current_context, ref_path),
            });
        }
    }
    try validatePropertySchemaKeywords(current_context, object);
}

fn validatePropertySchemaKeywords(context: SchemaContext, object: std.json.ObjectMap) anyerror!void {
    if (object.get("type")) |schema_type| {
        if (schema_type != .null) _ = try validateTypeSpecDefinition(schema_type, false);
    }
    if (object.get("format")) |format| {
        if (format != .null and format != .string) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("nullable")) |nullable| {
        if (nullable != .null and nullable != .bool) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("x-antfly-types")) |antfly_types| {
        if (antfly_types != .null) try validateAntflyTypesDefinition(antfly_types);
    }
    if (object.get("x-antfly-column-name")) |column_name| {
        if (column_name != .null and column_name != .string) return error.InvalidSchemaUpdateRequest;
        if (column_name == .string and !isSqlColumnAliasIdentifier(column_name.string)) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("x-antfly-analyzer")) |analyzer| {
        if (analyzer != .null and analyzer != .string) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("collation")) |collation| {
        if (collation != .null and collation != .string) return error.InvalidSchemaUpdateRequest;
        if (collation == .string and collation.string.len == 0) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("x-antfly-index")) |antfly_index| {
        if (antfly_index != .null and antfly_index != .bool) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("x-antfly-index-lifecycle")) |index_lifecycle| {
        if (index_lifecycle != .null and index_lifecycle != .string) return error.InvalidSchemaUpdateRequest;
        if (index_lifecycle == .string and RelationalIndexLifecycle.fromString(index_lifecycle.string) == null) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("x-antfly-index-generation")) |index_generation| {
        if (index_generation != .null and index_generation != .integer) return error.InvalidSchemaUpdateRequest;
        if (index_generation == .integer and index_generation.integer <= 0) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("x-antfly-index-name")) |index_name| {
        if (index_name != .null and index_name != .string) return error.InvalidSchemaUpdateRequest;
        if (index_name == .string and index_name.string.len == 0) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("x-antfly-index-access-method")) |index_access_method| {
        if (index_access_method != .null and index_access_method != .string) return error.InvalidSchemaUpdateRequest;
        if (index_access_method == .string and storage_schema.RelationalIndexAccessMethod.fromString(index_access_method.string) == null) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("x-antfly-index-schema-fingerprint")) |index_schema_fingerprint| {
        if (index_schema_fingerprint != .null and index_schema_fingerprint != .string) return error.InvalidSchemaUpdateRequest;
        if (index_schema_fingerprint == .string and index_schema_fingerprint.string.len == 0) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("x-antfly-index-include")) |index_include| {
        if (index_include != .null) try validateAntflyIncludeInAllDefinition(index_include);
    }
    if (object.get("x-antfly-index-keys")) |index_keys| {
        if (index_keys != .null) try validateRelationalIndexKeysJson(index_keys);
    }
    if (object.get("x-antfly-cardinality-proof")) |cardinality_proof| {
        if (cardinality_proof != .null and cardinality_proof != .string) return error.InvalidSchemaUpdateRequest;
        if (cardinality_proof == .string and storage_schema.RelationalColumnCardinalityProof.fromString(cardinality_proof.string) == null) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("x-antfly-index-where")) |index_where| {
        if (index_where != .null) try validateUniquePredicateDefinition(index_where);
    }
    if (object.get("x-antfly-index-where-expressions")) |index_where_expressions| {
        if (index_where_expressions != .null) try validateRelationalRowsExpressionConditionArrayJson(index_where_expressions);
    }
    if (object.get("x-antfly-include-in-all")) |include_in_all| {
        if (include_in_all != .null) try validateAntflyIncludeInAllDefinition(include_in_all);
    }
    if (object.get("schema")) |embedded_schema| {
        if (embedded_schema != .null) try validateDocumentSchemaDefinition(embedded_schema);
    }
    if (object.get("dynamic_templates")) |dynamic_templates| {
        if (dynamic_templates != .null) try validateDynamicTemplates(dynamic_templates);
    }
    if (object.get("$defs")) |definitions| {
        if (definitions != .null) try validateDefinitionsDefinition(context, definitions);
    }
    if (object.get("properties")) |properties| {
        if (properties != .null) {
            if (properties != .object) return error.InvalidSchemaUpdateRequest;
            var it = properties.object.iterator();
            while (it.next()) |entry| try validatePropertySchemaDefinitionWithContext(context, entry.value_ptr.*);
        }
    }
    if (object.get("required")) |required| {
        if (required != .null) {
            if (required != .array) return error.InvalidSchemaUpdateRequest;
            for (required.array.items) |entry| {
                if (entry != .string) return error.InvalidSchemaUpdateRequest;
            }
        }
    }
    if (object.get("propertyNames")) |property_names| {
        if (property_names != .null) try validatePropertySchemaDefinitionWithContext(context, property_names);
    }
    if (object.get("patternProperties")) |pattern_properties| {
        if (pattern_properties != .null) try validatePatternPropertiesDefinition(context, pattern_properties);
    }
    if (object.get("additionalProperties")) |additional_properties| {
        if (additional_properties != .null and additional_properties != .bool and additional_properties != .object) {
            return error.InvalidSchemaUpdateRequest;
        }
        if (additional_properties == .object) try validatePropertySchemaDefinitionWithContext(context, additional_properties);
    }
    if (object.get("unevaluatedProperties")) |unevaluated_properties| {
        if (unevaluated_properties != .null and unevaluated_properties != .bool and unevaluated_properties != .object) {
            return error.InvalidSchemaUpdateRequest;
        }
        if (unevaluated_properties == .object) try validatePropertySchemaDefinitionWithContext(context, unevaluated_properties);
    }
    if (object.get("dependentRequired")) |dependent_required| {
        if (dependent_required != .null) try validateDependentRequiredDefinition(dependent_required);
    }
    if (object.get("dependentSchemas")) |dependent_schemas| {
        if (dependent_schemas != .null) try validateDependentSchemasDefinition(context, dependent_schemas);
    }
    if (object.get("dependencies")) |dependencies| {
        if (dependencies != .null) try validateDependenciesDefinition(context, dependencies);
    }
    if (object.get("items")) |items| {
        if (items != .null) try validatePropertySchemaDefinitionWithContext(context, items);
    }
    if (object.get("prefixItems")) |prefix_items| {
        if (prefix_items != .null) try validatePrefixItemsDefinition(context, prefix_items);
    }
    try validateAdditionalItemsDefinition(context, object);
    if (object.get("unevaluatedItems")) |unevaluated_items| {
        if (unevaluated_items != .null and unevaluated_items != .bool and unevaluated_items != .object) {
            return error.InvalidSchemaUpdateRequest;
        }
        if (unevaluated_items == .object) try validatePropertySchemaDefinitionWithContext(context, unevaluated_items);
    }
    if (object.get("contains")) |contains| {
        if (contains != .null) try validatePropertySchemaDefinitionWithContext(context, contains);
    }
    if (object.get("const")) |_| {}
    if (object.get("enum")) |enum_values| {
        if (enum_values != .null) try validateEnumDefinition(enum_values);
    }
    if (object.get("minimum")) |minimum| {
        if (minimum != .null) _ = parseJsonNumber(minimum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("maximum")) |maximum| {
        if (maximum != .null) _ = parseJsonNumber(maximum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("exclusiveMinimum")) |exclusive_minimum| {
        if (exclusive_minimum != .null) _ = parseJsonNumber(exclusive_minimum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("exclusiveMaximum")) |exclusive_maximum| {
        if (exclusive_maximum != .null) _ = parseJsonNumber(exclusive_maximum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("multipleOf")) |multiple_of| {
        if (multiple_of != .null) try validatePositiveNumber(multiple_of);
    }
    if (object.get("anyOf")) |any_of| {
        if (any_of != .null) try validateAnyOfDefinition(context, any_of);
    }
    if (object.get("oneOf")) |one_of| {
        if (one_of != .null) try validateOneOfDefinition(context, one_of);
    }
    if (object.get("allOf")) |all_of| {
        if (all_of != .null) try validateAllOfDefinition(context, all_of);
    }
    if (object.get("not")) |not_schema| {
        if (not_schema != .null) try validatePropertySchemaDefinitionWithContext(context, not_schema);
    }
    if (object.get("if")) |if_schema| {
        if (if_schema != .null) try validatePropertySchemaDefinitionWithContext(context, if_schema);
    }
    if (object.get("then")) |then_schema| {
        if (then_schema != .null) try validatePropertySchemaDefinitionWithContext(context, then_schema);
    }
    if (object.get("else")) |else_schema| {
        if (else_schema != .null) try validatePropertySchemaDefinitionWithContext(context, else_schema);
    }
    if ((object.get("then") != null or object.get("else") != null) and object.get("if") == null) {
        return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("pattern")) |pattern| {
        if (pattern != .null and pattern != .string) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("minLength")) |min_length| {
        if (min_length != .null) try validateNonNegativeInteger(min_length);
    }
    if (object.get("maxLength")) |max_length| {
        if (max_length != .null) try validateNonNegativeInteger(max_length);
    }
    if (object.get("minProperties")) |min_properties| {
        if (min_properties != .null) try validateNonNegativeInteger(min_properties);
    }
    if (object.get("maxProperties")) |max_properties| {
        if (max_properties != .null) try validateNonNegativeInteger(max_properties);
    }
    if (object.get("minItems")) |min_items| {
        if (min_items != .null) try validateNonNegativeInteger(min_items);
    }
    if (object.get("maxItems")) |max_items| {
        if (max_items != .null) try validateNonNegativeInteger(max_items);
    }
    if (object.get("minContains")) |min_contains| {
        if (min_contains != .null) try validateNonNegativeInteger(min_contains);
    }
    if (object.get("maxContains")) |max_contains| {
        if (max_contains != .null) try validateNonNegativeInteger(max_contains);
    }
    if (object.get("uniqueItems")) |unique_items| {
        if (unique_items != .null and unique_items != .bool) return error.InvalidSchemaUpdateRequest;
    }
    if ((object.get("minContains") != null or object.get("maxContains") != null) and object.get("contains") == null) {
        return error.InvalidSchemaUpdateRequest;
    }
}

fn validateEnumDefinition(value: std.json.Value) !void {
    if (value != .array) return error.InvalidSchemaUpdateRequest;
}

fn validateAntflyTypesDefinition(value: std.json.Value) !void {
    if (value != .array) return error.InvalidSchemaUpdateRequest;
    for (value.array.items) |entry| {
        const type_name = switch (entry) {
            .string => |name| name,
            else => return error.InvalidSchemaUpdateRequest,
        };
        _ = try validateTypeName(type_name, false);
    }
}

fn validateAntflyIncludeInAllDefinition(value: std.json.Value) !void {
    switch (value) {
        .bool => {},
        .array => |arr| {
            for (arr.items) |entry| {
                if (entry != .string) return error.InvalidSchemaUpdateRequest;
            }
        },
        else => return error.InvalidSchemaUpdateRequest,
    }
}

fn validatePositiveNumber(value: std.json.Value) !void {
    const parsed = parseJsonNumber(value) catch return error.InvalidSchemaUpdateRequest;
    if (parsed <= 0) return error.InvalidSchemaUpdateRequest;
}

fn parseSchemaRefPath(value: std.json.Value) ![]const u8 {
    return switch (value) {
        .string => |ref_path| ref_path,
        else => error.InvalidSchemaUpdateRequest,
    };
}

fn isRootSchemaRef(ref_path: []const u8) bool {
    return std.mem.eql(u8, ref_path, "#");
}

fn hasRefSiblings(object: std.json.ObjectMap) bool {
    var it = object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "$ref")) continue;
        if (std.mem.eql(u8, entry.key_ptr.*, "$defs")) continue;
        return true;
    }
    return false;
}

fn resolveSchemaObject(context: SchemaContext, object: std.json.ObjectMap) !std.json.ObjectMap {
    if (object.get("$ref")) |ref_value| {
        const ref_path = try parseSchemaRefPath(ref_value);
        return try resolveSchemaRef(context, ref_path);
    }
    return object;
}

fn resolveSchemaRef(context: SchemaContext, ref_path: []const u8) !std.json.ObjectMap {
    if (isRootSchemaRef(ref_path)) return context.document_root;
    if (!std.mem.startsWith(u8, ref_path, "#/")) return error.InvalidSchemaUpdateRequest;

    return resolveSchemaRefWithin(context.scope_schema, ref_path) catch |scope_err| blk: {
        if (resolveSchemaRefWithin(context.document_root, ref_path)) |resolved| break :blk resolved else |root_err| {
            if (scope_err == error.InvalidSchemaUpdateRequest and root_err == error.InvalidSchemaUpdateRequest) {
                return error.InvalidSchemaUpdateRequest;
            }
            return root_err;
        }
    };
}

fn resolveSchemaRefWithin(root_schema: std.json.ObjectMap, ref_path: []const u8) !std.json.ObjectMap {
    var current: std.json.Value = .{ .object = root_schema };
    var tokens = std.mem.tokenizeScalar(u8, ref_path[2..], '/');
    while (tokens.next()) |token| {
        current = switch (current) {
            .object => |object| try resolveSchemaRefToken(object, token),
            else => return error.InvalidSchemaUpdateRequest,
        };
    }

    return switch (current) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
}

fn resolveSchemaRefToken(object: std.json.ObjectMap, token: []const u8) !std.json.Value {
    if (std.mem.indexOfScalar(u8, token, '~') == null) {
        return object.get(token) orelse return error.InvalidSchemaUpdateRequest;
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        if (try jsonPointerTokenMatches(token, entry.key_ptr.*)) return entry.value_ptr.*;
    }
    return error.InvalidSchemaUpdateRequest;
}

fn jsonPointerTokenMatches(encoded_token: []const u8, key: []const u8) !bool {
    var token_index: usize = 0;
    var key_index: usize = 0;
    while (token_index < encoded_token.len and key_index < key.len) {
        const decoded: u8 = blk: {
            switch (encoded_token[token_index]) {
                '~' => {
                    token_index += 1;
                    if (token_index >= encoded_token.len) return error.InvalidSchemaUpdateRequest;
                    break :blk switch (encoded_token[token_index]) {
                        '0' => @as(u8, '~'),
                        '1' => @as(u8, '/'),
                        else => return error.InvalidSchemaUpdateRequest,
                    };
                },
                else => break :blk encoded_token[token_index],
            }
        };
        if (decoded != key[key_index]) return false;
        token_index += 1;
        key_index += 1;
    }
    return token_index == encoded_token.len and key_index == key.len;
}

fn validateTypeSpecDefinition(value: std.json.Value, require_object_only: bool) !ParsedTypeSpec {
    return switch (value) {
        .string => |schema_type_name| blk: {
            const validated_type = try validateTypeName(schema_type_name, require_object_only);
            break :blk .{
                .field_type = validated_type,
                .integer_only = std.mem.eql(u8, validated_type, "integer"),
            };
        },
        .array => |schema_types| try validateTypeArrayDefinition(schema_types, require_object_only),
        else => return error.InvalidSchemaUpdateRequest,
    };
}

fn validateTypeArrayDefinition(value: std.json.Array, require_object_only: bool) !ParsedTypeSpec {
    var parsed = ParsedTypeSpec{};
    if (value.items.len == 0) return error.InvalidSchemaUpdateRequest;

    for (value.items) |entry| {
        const schema_type_name = switch (entry) {
            .string => |schema_type_name| schema_type_name,
            else => return error.InvalidSchemaUpdateRequest,
        };
        if (std.mem.eql(u8, schema_type_name, "null")) {
            if (parsed.allows_null) return error.InvalidSchemaUpdateRequest;
            parsed.allows_null = true;
            continue;
        }
        if (parsed.field_type != null) return error.InvalidSchemaUpdateRequest;
        parsed.field_type = try validateTypeName(schema_type_name, require_object_only);
        parsed.integer_only = std.mem.eql(u8, parsed.field_type.?, "integer");
    }

    if (parsed.field_type == null and !parsed.allows_null) return error.InvalidSchemaUpdateRequest;
    return parsed;
}

fn validateTypeName(schema_type_name: []const u8, require_object_only: bool) ![]const u8 {
    if (require_object_only) {
        if (!std.mem.eql(u8, schema_type_name, "object")) return error.InvalidSchemaUpdateRequest;
        return schema_type_name;
    }
    if (std.mem.eql(u8, schema_type_name, "text") or
        std.mem.eql(u8, schema_type_name, "keyword") or
        std.mem.eql(u8, schema_type_name, "link") or
        std.mem.eql(u8, schema_type_name, "blob") or
        std.mem.eql(u8, schema_type_name, "html") or
        std.mem.eql(u8, schema_type_name, "search_as_you_type") or
        std.mem.eql(u8, schema_type_name, "string") or
        std.mem.eql(u8, schema_type_name, "number") or
        std.mem.eql(u8, schema_type_name, "integer") or
        std.mem.eql(u8, schema_type_name, "null") or
        std.mem.eql(u8, schema_type_name, "numeric") or
        std.mem.eql(u8, schema_type_name, "embedding") or
        std.mem.eql(u8, schema_type_name, "boolean") or
        std.mem.eql(u8, schema_type_name, "datetime") or
        std.mem.eql(u8, schema_type_name, "geopoint") or
        std.mem.eql(u8, schema_type_name, "json") or
        std.mem.eql(u8, schema_type_name, "object") or
        std.mem.eql(u8, schema_type_name, "array"))
    {
        return schema_type_name;
    }
    return error.InvalidSchemaUpdateRequest;
}

fn validateDependentRequiredDefinition(value: std.json.Value) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var it = object.iterator();
    while (it.next()) |entry| {
        const required = switch (entry.value_ptr.*) {
            .array => |required| required,
            else => return error.InvalidSchemaUpdateRequest,
        };
        for (required.items) |required_value| {
            if (required_value != .string) return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn validateDefinitionsDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var it = object.iterator();
    while (it.next()) |entry| {
        try validatePropertySchemaDefinitionWithContext(context, entry.value_ptr.*);
    }
}

fn validateDependentSchemasDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var it = object.iterator();
    while (it.next()) |entry| {
        try validatePropertySchemaDefinitionWithContext(context, entry.value_ptr.*);
    }
}

fn validateDependenciesDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var it = object.iterator();
    while (it.next()) |entry| {
        switch (entry.value_ptr.*) {
            .array => {
                for (entry.value_ptr.array.items) |required_value| {
                    if (required_value != .string) return error.InvalidSchemaUpdateRequest;
                }
            },
            .object => try validatePropertySchemaDefinitionWithContext(context, entry.value_ptr.*),
            else => return error.InvalidSchemaUpdateRequest,
        }
    }
}

fn validatePatternPropertiesDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var it = object.iterator();
    while (it.next()) |entry| {
        try validatePropertySchemaDefinitionWithContext(context, entry.value_ptr.*);
    }
}

fn validatePrefixItemsDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    if (value != .array) return error.InvalidSchemaUpdateRequest;
    for (value.array.items) |item| try validatePropertySchemaDefinitionWithContext(context, item);
}

fn validateAnyOfDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    if (value != .array) return error.InvalidSchemaUpdateRequest;
    for (value.array.items) |variant| try validatePropertySchemaDefinitionWithContext(context, variant);
}

fn validateOneOfDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    if (value != .array) return error.InvalidSchemaUpdateRequest;
    for (value.array.items) |variant| try validatePropertySchemaDefinitionWithContext(context, variant);
}

fn validateAllOfDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    if (value != .array) return error.InvalidSchemaUpdateRequest;
    for (value.array.items) |variant| try validatePropertySchemaDefinitionWithContext(context, variant);
}

fn validateAdditionalItemsDefinition(context: SchemaContext, object: std.json.ObjectMap) anyerror!void {
    const additional_items = object.get("additionalItems") orelse return;
    if (additional_items == .null) return;
    if (object.get("prefixItems") == null) return error.InvalidSchemaUpdateRequest;
    if (object.get("items") != null) return error.InvalidSchemaUpdateRequest;
    if (additional_items != .bool and additional_items != .object) return error.InvalidSchemaUpdateRequest;
    if (additional_items == .object) try validatePropertySchemaDefinitionWithContext(context, additional_items);
}

fn validateDynamicTemplates(value: std.json.Value) !void {
    switch (value) {
        .object => |object| {
            var it = object.iterator();
            while (it.next()) |entry| try validateDynamicTemplate(entry.value_ptr.*);
        },
        .array => |array| {
            for (array.items) |entry| try validateDynamicTemplate(entry);
        },
        else => return error.InvalidSchemaUpdateRequest,
    }
}

fn validateDynamicTemplate(value: std.json.Value) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    if (object.get("match")) |match| if (match != .null and match != .string) return error.InvalidSchemaUpdateRequest;
    if (object.get("match_pattern")) |match_pattern| if (match_pattern != .null and match_pattern != .string) return error.InvalidSchemaUpdateRequest;
    if (object.get("unmatch")) |unmatch| if (unmatch != .null and unmatch != .string) return error.InvalidSchemaUpdateRequest;
    if (object.get("path_match")) |path_match| if (path_match != .null and path_match != .string) return error.InvalidSchemaUpdateRequest;
    if (object.get("path_unmatch")) |path_unmatch| if (path_unmatch != .null and path_unmatch != .string) return error.InvalidSchemaUpdateRequest;
    if (object.get("match_mapping_type")) |match_mapping_type| {
        if (match_mapping_type != .null and match_mapping_type != .string) return error.InvalidSchemaUpdateRequest;
        if (match_mapping_type == .string and
            !std.mem.eql(u8, match_mapping_type.string, "string") and
            !std.mem.eql(u8, match_mapping_type.string, "number") and
            !std.mem.eql(u8, match_mapping_type.string, "boolean") and
            !std.mem.eql(u8, match_mapping_type.string, "date") and
            !std.mem.eql(u8, match_mapping_type.string, "object"))
        {
            return error.InvalidSchemaUpdateRequest;
        }
    }

    const mapping = object.get("mapping") orelse return error.InvalidSchemaUpdateRequest;
    if (mapping == .null) return error.InvalidSchemaUpdateRequest;
    if (mapping != .object) return error.InvalidSchemaUpdateRequest;
    if (mapping.object.get("type")) |mapping_type| if (mapping_type != .null and mapping_type != .string) return error.InvalidSchemaUpdateRequest;
    if (mapping.object.get("analyzer")) |analyzer| if (analyzer != .null and analyzer != .string) return error.InvalidSchemaUpdateRequest;
    if (mapping.object.get("index")) |index| if (index != .null and index != .bool) return error.InvalidSchemaUpdateRequest;
    if (mapping.object.get("store")) |store| if (store != .null and store != .bool) return error.InvalidSchemaUpdateRequest;
    if (mapping.object.get("doc_values")) |doc_values| if (doc_values != .null and doc_values != .bool) return error.InvalidSchemaUpdateRequest;
    if (mapping.object.get("include_in_all")) |include_in_all| if (include_in_all != .null and include_in_all != .bool) return error.InvalidSchemaUpdateRequest;
}

fn validateNonNegativeInteger(value: std.json.Value) !void {
    const integer = switch (value) {
        .integer => |integer| integer,
        else => return error.InvalidSchemaUpdateRequest,
    };
    if (integer < 0) return error.InvalidSchemaUpdateRequest;
}

fn parseTableSchemaValue(alloc: std.mem.Allocator, value: std.json.Value) !TableSchema {
    const root = value.object;

    var parsed: TableSchema = .{
        .default_type = try alloc.dupe(u8, ""),
        .ttl_field = try alloc.dupe(u8, "_timestamp"),
    };
    errdefer parsed.deinit(alloc);

    if (root.get("version")) |version| {
        if (version != .null) parsed.version = std.math.cast(u32, version.integer) orelse return error.InvalidSchemaUpdateRequest;
    }
    if (root.get("storage_mode")) |storage_mode| {
        if (storage_mode != .null) {
            if (storage_mode != .string) return error.InvalidSchemaUpdateRequest;
            parsed.storage_mode = StorageMode.fromString(storage_mode.string) orelse return error.InvalidSchemaUpdateRequest;
        }
    }
    if (root.get("default_type")) |default_type| {
        if (default_type != .null) {
            alloc.free(parsed.default_type);
            parsed.default_type = try alloc.dupe(u8, default_type.string);
        }
    }
    if (root.get("ttl_duration_ns")) |ttl_duration_ns| {
        if (ttl_duration_ns != .null) parsed.ttl_duration_ns = std.math.cast(u64, ttl_duration_ns.integer) orelse return error.InvalidSchemaUpdateRequest;
    }
    if (root.get("ttl_field")) |ttl_field| {
        if (ttl_field != .null) {
            if (ttl_field.string.len == 0) return error.InvalidSchemaUpdateRequest;
            alloc.free(parsed.ttl_field);
            parsed.ttl_field = try alloc.dupe(u8, ttl_field.string);
        }
    }
    if (root.get("enforce_types")) |enforce_types| {
        if (enforce_types != .null) parsed.enforce_types = enforce_types.bool;
    }
    if (root.get("document_schemas")) |document_schemas| {
        if (document_schemas != .null) parsed.document_schemas = try parseDocumentSchemas(alloc, document_schemas);
    }
    if (root.get("dynamic_templates")) |dynamic_templates| {
        if (dynamic_templates != .null) parsed.dynamic_templates = try parseDynamicTemplates(alloc, dynamic_templates);
    }
    if (root.get("primary_key")) |primary_key| {
        if (primary_key != .null) parsed.primary_key = try parsePrimaryKey(alloc, primary_key);
    }
    if (root.get("periods")) |periods| {
        if (periods != .null) parsed.periods = try parseRelationalPeriods(alloc, periods);
    }
    if (root.get("foreign_keys")) |foreign_keys| {
        if (foreign_keys != .null) parsed.foreign_keys = try parseForeignKeys(alloc, foreign_keys);
    }
    if (root.get("unique_constraints")) |constraints| {
        if (constraints != .null) parsed.unique_constraints = try parseUniqueConstraints(alloc, constraints);
    }
    if (root.get("relational_indexes")) |indexes| {
        if (indexes != .null) parsed.relational_indexes = try parseRelationalIndexes(alloc, indexes);
    }
    if (root.get("checks")) |checks| {
        if (checks != .null) parsed.checks = try parseRelationalChecks(alloc, checks);
    }
    if (root.get("system_versioned")) |system_versioned| {
        if (system_versioned != .null) parsed.system_versioned = system_versioned.bool;
    }
    if (root.get("base_source")) |base_source| {
        if (base_source != .null) parsed.external_base_source = try parseExternalBaseSource(alloc, base_source);
    } else if (root.get("external_base_source")) |base_source| {
        if (base_source != .null) parsed.external_base_source = try parseExternalBaseSource(alloc, base_source);
    }
    if (parsed.storage_mode == .relational) {
        if (root.get("enforce_types")) |enforce_types| {
            if (enforce_types != .null and !enforce_types.bool) return error.InvalidSchemaUpdateRequest;
        }
        parsed.enforce_types = true;
    }
    return parsed;
}

fn validateParsedTtlSchema(schema: TableSchema) !void {
    if (schema.ttl_duration_ns == 0) return;
    if (schema.ttl_field.len == 0) return error.InvalidSchemaUpdateRequest;

    for (schema.document_schemas) |document_schema| {
        if (findDocumentProperty(document_schema.properties, schema.ttl_field)) |property| {
            const field_type = property.field_type orelse return error.InvalidSchemaUpdateRequest;
            if (!std.mem.eql(u8, field_type, "datetime") and !std.mem.eql(u8, field_type, "numeric")) {
                return error.InvalidSchemaUpdateRequest;
            }
        }
    }
}

fn validateParsedDocumentJoinCardinalityProofs(schema: TableSchema) !void {
    for (schema.document_schemas) |document_schema| {
        for (document_schema.properties) |property| {
            try validateDocumentJoinCardinalityProofProperty(property, true);
        }
    }
}

fn validateDocumentJoinCardinalityProofProperty(
    property: DocumentProperty,
    materializes_runtime_column: bool,
) !void {
    if (property.cardinality_proof != .none) {
        if (!materializes_runtime_column) return error.InvalidSchemaUpdateRequest;
        if (!isDocumentJoinCardinalityProofScalar(property)) return error.InvalidSchemaUpdateRequest;
        if (isReservedDocumentSqlField(property.name)) return error.InvalidSchemaUpdateRequest;
        if (property.sql_column_name) |column_name| {
            if (isReservedDocumentSqlField(column_name)) return error.InvalidSchemaUpdateRequest;
        }
        if (property.generated != null) return error.InvalidSchemaUpdateRequest;
        if (property.antfly_index != null and !property.antfly_index.?) return error.InvalidSchemaUpdateRequest;
        if (property.index_lifecycle != null and property.index_lifecycle.? != .ready) return error.InvalidSchemaUpdateRequest;
        if (property.index_where.len != 0 or property.index_where_expressions.len != 0) return error.InvalidSchemaUpdateRequest;
        if (property.index_keys.len > 1) return error.InvalidSchemaUpdateRequest;
        if (property.index_keys.len == 1) {
            const key = property.index_keys[0];
            const column_name = property.sql_column_name orelse property.name;
            if (!std.mem.eql(u8, key.column, column_name)) return error.InvalidSchemaUpdateRequest;
            if (key.direction != .asc or key.nulls != .default) return error.InvalidSchemaUpdateRequest;
        }
    }

    for (property.properties) |child| {
        try validateDocumentJoinCardinalityProofProperty(child, child.sql_column_name != null);
    }
    if (property.item) |item| try validateDocumentJoinCardinalityProofProperty(item.*, false);
    if (property.additional_properties_schema) |child| try validateDocumentJoinCardinalityProofProperty(child.*, false);
    if (property.unevaluated_properties_schema) |child| try validateDocumentJoinCardinalityProofProperty(child.*, false);
    if (property.unevaluated_items_schema) |child| try validateDocumentJoinCardinalityProofProperty(child.*, false);
    for (property.prefix_items) |child| try validateDocumentJoinCardinalityProofProperty(child, false);
    for (property.pattern_properties) |child| try validateDocumentJoinCardinalityProofProperty(child.property.*, false);
    for (property.dependent_schemas) |dependency| try validateDocumentJoinCardinalityProofProperty(dependency.schema.*, false);
    for (property.any_of) |child| try validateDocumentJoinCardinalityProofProperty(child, false);
    for (property.one_of) |child| try validateDocumentJoinCardinalityProofProperty(child, false);
    for (property.all_of) |child| try validateDocumentJoinCardinalityProofProperty(child, false);
    if (property.not_schema) |child| try validateDocumentJoinCardinalityProofProperty(child.*, false);
    if (property.if_schema) |child| try validateDocumentJoinCardinalityProofProperty(child.*, false);
    if (property.then_schema) |child| try validateDocumentJoinCardinalityProofProperty(child.*, false);
    if (property.else_schema) |child| try validateDocumentJoinCardinalityProofProperty(child.*, false);
    if (property.contains_schema) |child| try validateDocumentJoinCardinalityProofProperty(child.*, false);
}

fn isReservedDocumentSqlField(name: []const u8) bool {
    return std.ascii.eqlIgnoreCase(name, "_id") or
        std.ascii.eqlIgnoreCase(name, "_doc") or
        std.ascii.eqlIgnoreCase(name, "_version");
}

fn isDocumentJoinCardinalityProofScalar(property: DocumentProperty) bool {
    const field_type = property.field_type orelse return false;
    return std.mem.eql(u8, field_type, "keyword") or
        std.mem.eql(u8, field_type, "link") or
        std.mem.eql(u8, field_type, "string") or
        std.mem.eql(u8, field_type, "text") or
        std.mem.eql(u8, field_type, "boolean") or
        std.mem.eql(u8, field_type, "datetime") or
        std.mem.eql(u8, field_type, "integer") or
        std.mem.eql(u8, field_type, "numeric") or
        std.mem.eql(u8, field_type, "number");
}

fn validateParsedRelationalSchema(schema: TableSchema) !void {
    if (schema.storage_mode != .relational) {
        if (schema.primary_key != null or schema.periods.len != 0 or schema.foreign_keys.len != 0 or schema.unique_constraints.len != 0 or schema.relational_indexes.len != 0 or schema.checks.len != 0 or schema.external_base_source != null or schema.system_versioned) return error.InvalidSchemaUpdateRequest;
        return;
    }
    if (!schema.enforce_types) return error.InvalidSchemaUpdateRequest;
    if (schema.dynamic_templates.len > 0) return error.InvalidSchemaUpdateRequest;
    if (schema.document_schemas.len != 1) return error.InvalidSchemaUpdateRequest;

    var relational_columns: usize = 0;
    for (schema.document_schemas) |document_schema| {
        if (document_schema.additional_properties_schema != null) return error.InvalidSchemaUpdateRequest;
        if (document_schema.additional_properties_allowed orelse false) return error.InvalidSchemaUpdateRequest;
        if (document_schema.pattern_properties.len > 0) return error.InvalidSchemaUpdateRequest;
        if (document_schema.dynamic_infer_types) return error.InvalidSchemaUpdateRequest;

        for (document_schema.properties) |property| {
            try validateRelationalEmbeddedJsonProperty(property);
            try validateRelationalGeneratedProperty(schema, document_schema, property);
            try validateRelationalPartialIndexProperty(schema, property);
            if (isRelationalStorageProperty(property)) relational_columns += 1;
        }
    }

    if (relational_columns == 0) return error.InvalidSchemaUpdateRequest;
    try validateRelationalPeriods(schema);
    try validateRelationalPrimaryKey(schema);
    try validateRelationalUniqueConstraints(schema);
    try validateRelationalIndexesForSchema(schema);
    try validateRelationalChecks(schema);
    try validateRelationalForeignKeys(schema);
}

fn validateRelationalPrimaryKey(schema: TableSchema) !void {
    const primary_key = schema.primary_key orelse return;
    if (primary_key.timing == .deferred and !primary_key.deferrable) return error.InvalidSchemaUpdateRequest;
    if (primary_key.name) |name| {
        for (schema.unique_constraints) |constraint| {
            if (std.mem.eql(u8, constraint.name, name)) return error.InvalidSchemaUpdateRequest;
        }
        for (schema.foreign_keys) |foreign_key| {
            if (std.mem.eql(u8, foreign_key.name, name)) return error.InvalidSchemaUpdateRequest;
        }
        for (schema.checks) |check| {
            if (std.mem.eql(u8, check.name, name)) return error.InvalidSchemaUpdateRequest;
        }
    }
    if (primary_key.without_overlaps_period) |period| try validateRelationalPeriodReference(schema, period);
    for (primary_key.columns, 0..) |column, column_index| {
        if (std.mem.eql(u8, column, "_id")) return error.InvalidSchemaUpdateRequest;
        const property = findDocumentProperty(schema.document_schemas[0].properties, column) orelse return error.InvalidSchemaUpdateRequest;
        if (!isRelationalUniqueConstraintColumn(property)) return error.InvalidSchemaUpdateRequest;
        if (!requiredFieldsContain(schema.document_schemas[0].required_fields, column)) return error.InvalidSchemaUpdateRequest;
        for (primary_key.columns[0..column_index]) |previous_column| {
            if (std.mem.eql(u8, previous_column, column)) return error.InvalidSchemaUpdateRequest;
        }
    }
    for (primary_key.include_columns, 0..) |column, column_index| {
        if (stringSlicesContains(primary_key.columns, column)) return error.InvalidSchemaUpdateRequest;
        const property = findDocumentProperty(schema.document_schemas[0].properties, column) orelse return error.InvalidSchemaUpdateRequest;
        if (!isRelationalUniqueConstraintColumn(property)) return error.InvalidSchemaUpdateRequest;
        for (primary_key.include_columns[0..column_index]) |previous_column| {
            if (std.mem.eql(u8, previous_column, column)) return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn validateRelationalPeriods(schema: TableSchema) !void {
    for (schema.periods, 0..) |period, i| {
        if (period.name.len == 0 or period.start_column.len == 0 or period.end_column.len == 0) return error.InvalidSchemaUpdateRequest;
        if (std.mem.eql(u8, period.start_column, period.end_column)) return error.InvalidSchemaUpdateRequest;
        const start_property = findDocumentProperty(schema.document_schemas[0].properties, period.start_column) orelse return error.InvalidSchemaUpdateRequest;
        const end_property = findDocumentProperty(schema.document_schemas[0].properties, period.end_column) orelse return error.InvalidSchemaUpdateRequest;
        if (!isRelationalPeriodColumn(start_property) or !isRelationalPeriodColumn(end_property)) return error.InvalidSchemaUpdateRequest;
        if (!relationalConstraintColumnTypesCompatible(start_property, end_property)) return error.InvalidSchemaUpdateRequest;
        if (period.range_type) |range_type| try validateRelationalPeriodRangeType(range_type, start_property, end_property);
        for (schema.periods[0..i]) |previous| {
            if (std.mem.eql(u8, previous.name, period.name)) return error.InvalidSchemaUpdateRequest;
            if (std.mem.eql(u8, previous.start_column, period.start_column) or
                std.mem.eql(u8, previous.start_column, period.end_column) or
                std.mem.eql(u8, previous.end_column, period.start_column) or
                std.mem.eql(u8, previous.end_column, period.end_column))
            {
                return error.InvalidSchemaUpdateRequest;
            }
        }
    }
}

fn validateRelationalPeriodReference(schema: TableSchema, period_name: []const u8) !void {
    _ = findRelationalPeriod(schema.periods, period_name) orelse return error.InvalidSchemaUpdateRequest;
}

fn findRelationalPeriod(periods: []const RelationalPeriod, name: []const u8) ?RelationalPeriod {
    for (periods) |period| {
        if (std.mem.eql(u8, period.name, name)) return period;
    }
    return null;
}

fn isRelationalPeriodColumn(property: DocumentProperty) bool {
    const field_type = property.field_type orelse return false;
    return std.mem.eql(u8, field_type, "numeric") or
        std.mem.eql(u8, field_type, "number") or
        std.mem.eql(u8, field_type, "integer") or
        std.mem.eql(u8, field_type, "datetime");
}

fn validateRelationalForeignKeys(schema: TableSchema) !void {
    for (schema.foreign_keys, 0..) |foreign_key, i| {
        if (foreign_key.columns.len == 0) return error.InvalidSchemaUpdateRequest;
        if (foreign_key.references.columns.len == 0) return error.InvalidSchemaUpdateRequest;
        if (foreign_key.validation_state == .validating or foreign_key.validation_state == .invalid) return error.InvalidSchemaUpdateRequest;
        if (foreign_key.on_update != .restrict and foreign_key.on_update != .no_action and foreign_key.on_update != .set_null and foreign_key.on_update != .cascade) return error.InvalidSchemaUpdateRequest;
        if (foreign_key.timing == .deferred and !foreign_key.deferrable) return error.InvalidSchemaUpdateRequest;
        if (foreign_key.match == .partial) return error.InvalidSchemaUpdateRequest;
        if (foreign_key.period) |period| try validateRelationalPeriodReference(schema, period);
        if ((foreign_key.period == null) != (foreign_key.references.period == null)) return error.InvalidSchemaUpdateRequest;
        if (foreign_key.references.period) |period| {
            if (!foreignKeyActionSupportsTemporalUpdate(foreign_key.on_update)) return error.InvalidSchemaUpdateRequest;
            const same_table_parent = std.mem.eql(u8, foreign_key.references.table, schema.default_type);
            if (same_table_parent) try validateRelationalPeriodReference(schema, period);
        }
        if (foreignKeyReferencesPrimaryKey(foreign_key)) {
            if (foreign_key.columns.len != 1) return error.InvalidSchemaUpdateRequest;
            const child_property = findDocumentProperty(schema.document_schemas[0].properties, foreign_key.columns[0]) orelse return error.InvalidSchemaUpdateRequest;
            if (!isRelationalForeignKeyColumn(child_property)) return error.InvalidSchemaUpdateRequest;
        } else {
            if (foreignKeyReferencesPrimaryKeyComponent(foreign_key)) return error.InvalidSchemaUpdateRequest;
            if (foreign_key.columns.len != foreign_key.references.columns.len) return error.InvalidSchemaUpdateRequest;
            const same_table_parent = std.mem.eql(u8, foreign_key.references.table, schema.default_type);
            const parent_primary = same_table_parent and primaryKeyColumnsEqual(schema.primary_key, foreign_key.references.columns);
            const parent_unique = if (same_table_parent and !parent_primary) findUniqueConstraintByColumns(schema.unique_constraints, foreign_key.references.columns) orelse return error.InvalidSchemaUpdateRequest else null;
            for (foreign_key.columns, foreign_key.references.columns) |child_column, parent_column| {
                const child_property = findDocumentProperty(schema.document_schemas[0].properties, child_column) orelse return error.InvalidSchemaUpdateRequest;
                if (!isRelationalUniqueConstraintColumn(child_property)) return error.InvalidSchemaUpdateRequest;
                if (parent_unique != null or parent_primary) {
                    const parent_property = findDocumentProperty(schema.document_schemas[0].properties, parent_column) orelse return error.InvalidSchemaUpdateRequest;
                    if (!relationalConstraintColumnTypesCompatible(child_property, parent_property)) return error.InvalidSchemaUpdateRequest;
                }
            }
        }
        if (foreign_key.on_delete == .set_null or foreign_key.on_update == .set_null) {
            for (foreign_key.columns) |column| {
                if (requiredFieldsContain(schema.document_schemas[0].required_fields, column)) return error.InvalidSchemaUpdateRequest;
            }
        }
        for (foreign_key.columns, 0..) |column, column_index| {
            for (foreign_key.columns[0..column_index]) |previous_column| {
                if (std.mem.eql(u8, previous_column, column)) return error.InvalidSchemaUpdateRequest;
            }
        }
        for (schema.foreign_keys[0..i]) |previous| {
            if (std.mem.eql(u8, previous.name, foreign_key.name)) return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn foreignKeyActionIsRestrictive(action: ForeignKeyAction) bool {
    return action == .restrict or action == .no_action;
}

fn foreignKeyActionSupportsTemporalUpdate(action: ForeignKeyAction) bool {
    return foreignKeyActionIsRestrictive(action) or action == .set_null or action == .cascade;
}

fn foreignKeyReferencesPrimaryKey(foreign_key: ForeignKey) bool {
    return foreign_key.references.columns.len == 1 and std.mem.eql(u8, foreign_key.references.columns[0], "_id");
}

fn foreignKeyReferencesPrimaryKeyComponent(foreign_key: ForeignKey) bool {
    for (foreign_key.references.columns) |column| {
        if (std.mem.eql(u8, column, "_id")) return true;
    }
    return false;
}

fn primaryKeyColumnsEqual(primary_key: ?PrimaryKey, columns: []const []const u8) bool {
    const key = primary_key orelse return false;
    return stringSlicesEqual(key.columns, columns);
}

fn requiredFieldsContain(required_fields: []const []const u8, name: []const u8) bool {
    for (required_fields) |field_name| {
        if (std.mem.eql(u8, field_name, name)) return true;
    }
    return false;
}

fn findUniqueConstraintByColumns(constraints: []const UniqueConstraint, columns: []const []const u8) ?UniqueConstraint {
    for (constraints) |constraint| {
        if (constraint.where.len != 0) continue;
        if (constraint.expressions.len != 0) continue;
        if (stringSlicesEqual(constraint.columns, columns)) return constraint;
    }
    return null;
}

fn findUniqueConstraintByName(constraints: []const UniqueConstraint, name: []const u8) ?UniqueConstraint {
    for (constraints) |constraint| {
        if (std.mem.eql(u8, constraint.name, name)) return constraint;
    }
    return null;
}

fn relationalConstraintColumnTypesCompatible(child: DocumentProperty, parent: DocumentProperty) bool {
    const child_type = child.field_type orelse return false;
    const parent_type = parent.field_type orelse return false;
    return std.mem.eql(u8, child_type, parent_type);
}

fn validateRelationalUniqueConstraints(schema: TableSchema) !void {
    for (schema.unique_constraints, 0..) |constraint, i| {
        if (constraint.timing == .deferred and !constraint.deferrable) return error.InvalidSchemaUpdateRequest;
        if (constraint.columns.len + constraint.expressions.len == 0) return error.InvalidSchemaUpdateRequest;
        if (constraint.validation_state == .validating or constraint.validation_state == .invalid) return error.InvalidSchemaUpdateRequest;
        if (constraint.without_overlaps_period) |period| try validateRelationalPeriodReference(schema, period);
        if (constraint.nulls_not_distinct and constraint.without_overlaps_period != null) return error.InvalidSchemaUpdateRequest;
        for (constraint.columns, 0..) |column, column_index| {
            const property = findDocumentProperty(schema.document_schemas[0].properties, column) orelse return error.InvalidSchemaUpdateRequest;
            if (!isRelationalUniqueConstraintColumn(property)) return error.InvalidSchemaUpdateRequest;
            for (constraint.columns[0..column_index]) |previous_column| {
                if (std.mem.eql(u8, previous_column, column)) return error.InvalidSchemaUpdateRequest;
            }
        }
        for (constraint.expressions, 0..) |expression, expression_index| {
            try validateRelationalUniqueConstraintExpression(schema, expression);
            for (constraint.expressions[0..expression_index]) |previous_expression| {
                if (uniqueExpressionsEqual(previous_expression, expression)) return error.InvalidSchemaUpdateRequest;
            }
        }
        for (constraint.include_columns, 0..) |column, column_index| {
            if (stringSlicesContains(constraint.columns, column)) return error.InvalidSchemaUpdateRequest;
            const property = findDocumentProperty(schema.document_schemas[0].properties, column) orelse return error.InvalidSchemaUpdateRequest;
            if (!isRelationalStorageProperty(property)) return error.InvalidSchemaUpdateRequest;
            for (constraint.include_columns[0..column_index]) |previous_column| {
                if (std.mem.eql(u8, previous_column, column)) return error.InvalidSchemaUpdateRequest;
            }
        }
        for (constraint.where) |predicate| try validateRelationalUniquePredicate(schema, predicate);
        for (constraint.where_expressions) |condition| try validateRelationalUniquePredicateExpression(schema, condition);
        for (schema.unique_constraints[0..i]) |previous| {
            if (std.mem.eql(u8, previous.name, constraint.name)) return error.InvalidSchemaUpdateRequest;
            if (uniqueConstraintsEquivalent(previous, constraint)) return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn validateRelationalUniqueConstraintExpression(schema: TableSchema, expression: UniqueExpression) !void {
    switch (expression.op) {
        .lower, .upper, .md5 => {
            if (expression.field.len == 0 or expression.expression != null) return error.InvalidSchemaUpdateRequest;
            const property = findDocumentProperty(schema.document_schemas[0].properties, expression.field) orelse return error.InvalidSchemaUpdateRequest;
            const field_type = property.field_type orelse return error.InvalidSchemaUpdateRequest;
            if (!std.mem.eql(u8, field_type, "keyword") and
                !std.mem.eql(u8, field_type, "link") and
                !std.mem.eql(u8, field_type, "string") and
                !std.mem.eql(u8, field_type, "text"))
            {
                return error.InvalidSchemaUpdateRequest;
            }
        },
        .expression => {
            if (expression.field.len != 0) return error.InvalidSchemaUpdateRequest;
            const row_expression = expression.expression orelse return error.InvalidSchemaUpdateRequest;
            if (!relationalRowsExpressionDeterministic(row_expression)) return error.InvalidSchemaUpdateRequest;
            try validateRelationalRowsExpressionAgainstSchema(schema, row_expression);
            switch (try relationalRowsExpressionType(schema, row_expression)) {
                .text, .numeric, .boolean, .datetime => {},
                .json, .array, .null => return error.InvalidSchemaUpdateRequest,
            }
        },
    }
}

fn validateRelationalIndexKeys(schema: TableSchema, keys: []const storage_schema.RelationalIndexKey) !void {
    for (keys, 0..) |key, key_index| {
        if (key.column.len == 0) return error.InvalidSchemaUpdateRequest;
        const property = findDocumentProperty(schema.document_schemas[0].properties, key.column) orelse return error.InvalidSchemaUpdateRequest;
        if (!isRelationalStorageProperty(property)) return error.InvalidSchemaUpdateRequest;
        for (keys[0..key_index]) |previous| {
            if (std.mem.eql(u8, previous.column, key.column)) return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn validateRelationalIndexesForSchema(schema: TableSchema) !void {
    for (schema.relational_indexes, 0..) |index, i| {
        if (index.name.len == 0 or index.owner_name.len == 0) return error.InvalidSchemaUpdateRequest;
        if (index.generation == 0 and index.schema_fingerprint != null) return error.InvalidSchemaUpdateRequest;
        if (index.generation != 0 and index.schema_fingerprint == null) return error.InvalidSchemaUpdateRequest;
        switch (index.access_method) {
            .scalar_column, .algebraic_filter, .text_search => if (index.keys.len != 0) return error.InvalidSchemaUpdateRequest,
            .ordered_tuple => if (index.keys.len == 0 and !(index.owner_kind == .unique_constraint and index.expressions.len != 0)) return error.InvalidSchemaUpdateRequest,
        }
        switch (index.access_method) {
            .scalar_column, .ordered_tuple => if (index.method_config_json != null) return error.InvalidSchemaUpdateRequest,
            .text_search => if (index.method_config_json == null or index.owner_kind != .relational_column) return error.InvalidSchemaUpdateRequest,
            .algebraic_filter => if (index.owner_kind != .table and index.method_config_json != null) return error.InvalidSchemaUpdateRequest,
        }
        for (index.columns, 0..) |column, column_index| {
            const property = findDocumentProperty(schema.document_schemas[0].properties, column) orelse return error.InvalidSchemaUpdateRequest;
            if (!isRelationalStorageProperty(property)) return error.InvalidSchemaUpdateRequest;
            for (index.columns[0..column_index]) |previous| {
                if (std.mem.eql(u8, previous, column)) return error.InvalidSchemaUpdateRequest;
            }
        }
        for (index.expressions, 0..) |expression, expression_index| {
            try validateRelationalStorageUniqueExpression(schema, expression);
            for (index.expressions[0..expression_index]) |previous| {
                if (storageUniqueExpressionsEqual(previous, expression)) return error.InvalidSchemaUpdateRequest;
            }
        }
        if (index.owner_kind != .unique_constraint and index.expressions.len != 0) return error.InvalidSchemaUpdateRequest;
        if (index.owner_kind != .table and index.columns.len == 0 and !(index.owner_kind == .unique_constraint and index.expressions.len != 0)) return error.InvalidSchemaUpdateRequest;
        for (index.include_columns, 0..) |column, column_index| {
            if (stringSlicesContains(index.columns, column)) return error.InvalidSchemaUpdateRequest;
            const property = findDocumentProperty(schema.document_schemas[0].properties, column) orelse return error.InvalidSchemaUpdateRequest;
            if (!isRelationalStorageProperty(property)) return error.InvalidSchemaUpdateRequest;
            for (index.include_columns[0..column_index]) |previous| {
                if (std.mem.eql(u8, previous, column)) return error.InvalidSchemaUpdateRequest;
            }
        }
        try validateRelationalIndexKeys(schema, index.keys);
        for (index.where) |predicate| try validateRelationalStorageUniquePredicate(schema, predicate);
        for (index.where_expressions) |condition| try validateRelationalUniquePredicateExpression(schema, condition);
        switch (index.owner_kind) {
            .relational_column => {
                if (index.unique) return error.InvalidSchemaUpdateRequest;
                const property = findDocumentProperty(schema.document_schemas[0].properties, index.owner_name) orelse return error.InvalidSchemaUpdateRequest;
                if (!isRelationalStorageProperty(property)) return error.InvalidSchemaUpdateRequest;
                if (!stringSlicesContains(index.columns, index.owner_name)) return error.InvalidSchemaUpdateRequest;
            },
            .unique_constraint => {
                if (!index.unique) return error.InvalidSchemaUpdateRequest;
                const constraint = findUniqueConstraintByName(schema.unique_constraints, index.owner_name) orelse return error.InvalidSchemaUpdateRequest;
                if (!stringSlicesEqual(index.columns, constraint.columns)) return error.InvalidSchemaUpdateRequest;
                if (!storageUniqueExpressionSlicesEqual(index.expressions, constraint.expressions)) return error.InvalidSchemaUpdateRequest;
            },
            .table => {
                if (index.unique) return error.InvalidSchemaUpdateRequest;
                if (!std.mem.eql(u8, index.owner_name, storage_schema.relational_table_index_owner_name)) return error.InvalidSchemaUpdateRequest;
                if (index.access_method != .algebraic_filter) return error.InvalidSchemaUpdateRequest;
                if (index.method_config_json == null) return error.InvalidSchemaUpdateRequest;
                if (index.columns.len != 0 or index.include_columns.len != 0 or index.keys.len != 0) return error.InvalidSchemaUpdateRequest;
                if (index.where.len != 0 or index.where_expressions.len != 0) return error.InvalidSchemaUpdateRequest;
            },
        }
        for (schema.relational_indexes[0..i]) |previous| {
            if (std.mem.eql(u8, previous.name, index.name)) return error.InvalidSchemaUpdateRequest;
            if (index.owner_kind != .table and previous.owner_kind == index.owner_kind and std.mem.eql(u8, previous.owner_name, index.owner_name)) return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn validateRelationalStorageUniqueExpression(schema: TableSchema, expression: storage_schema.UniqueExpression) !void {
    switch (expression.op) {
        .lower, .upper, .md5 => {
            if (expression.field.len == 0 or expression.expression != null) return error.InvalidSchemaUpdateRequest;
            const property = findDocumentProperty(schema.document_schemas[0].properties, expression.field) orelse return error.InvalidSchemaUpdateRequest;
            const field_type = property.field_type orelse return error.InvalidSchemaUpdateRequest;
            if (!std.mem.eql(u8, field_type, "keyword") and
                !std.mem.eql(u8, field_type, "link") and
                !std.mem.eql(u8, field_type, "string") and
                !std.mem.eql(u8, field_type, "text"))
            {
                return error.InvalidSchemaUpdateRequest;
            }
        },
        .expression => {
            if (expression.field.len != 0) return error.InvalidSchemaUpdateRequest;
            const row_expression = expression.expression orelse return error.InvalidSchemaUpdateRequest;
            if (!relationalRowsExpressionDeterministic(row_expression)) return error.InvalidSchemaUpdateRequest;
            try validateRelationalRowsExpressionAgainstSchema(schema, row_expression);
            switch (try relationalRowsExpressionType(schema, row_expression)) {
                .text, .numeric, .boolean, .datetime => {},
                .json, .array, .null => return error.InvalidSchemaUpdateRequest,
            }
        },
    }
}

fn validateRelationalStorageUniquePredicate(schema: TableSchema, predicate: storage_schema.UniquePredicate) !void {
    const property = findDocumentProperty(schema.document_schemas[0].properties, predicate.field) orelse return error.InvalidSchemaUpdateRequest;
    if (!isRelationalUniqueConstraintColumn(property)) return error.InvalidSchemaUpdateRequest;
    switch (predicate.op) {
        .is_null, .is_not_null => if (predicate.value_json != null) return error.InvalidSchemaUpdateRequest,
        .eq, .ne => if (predicate.value_json == null) return error.InvalidSchemaUpdateRequest,
    }
}

fn storageUniqueExpressionsEqual(a: storage_schema.UniqueExpression, b: storage_schema.UniqueExpression) bool {
    if (a.op != b.op or !std.mem.eql(u8, a.field, b.field)) return false;
    if ((a.expression == null) != (b.expression == null)) return false;
    if (a.expression) |a_expression| {
        return relationalRowsExpressionsEqual(a_expression, b.expression.?);
    }
    return true;
}

fn storageUniqueExpressionSlicesEqual(a: []const storage_schema.UniqueExpression, b: []const UniqueExpression) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (left.op != switch (right.op) {
            .lower => storage_schema.UniqueExpressionOp.lower,
            .upper => storage_schema.UniqueExpressionOp.upper,
            .md5 => storage_schema.UniqueExpressionOp.md5,
            .expression => storage_schema.UniqueExpressionOp.expression,
        }) return false;
        if (!std.mem.eql(u8, left.field, right.field)) return false;
        if ((left.expression == null) != (right.expression == null)) return false;
        if (left.expression) |left_expression| {
            if (!relationalRowsExpressionsEqual(left_expression, right.expression.?)) return false;
        }
    }
    return true;
}

fn validateRelationalUniquePredicate(schema: TableSchema, predicate: UniquePredicate) !void {
    const property = findDocumentProperty(schema.document_schemas[0].properties, predicate.field) orelse return error.InvalidSchemaUpdateRequest;
    if (!isRelationalUniqueConstraintColumn(property)) return error.InvalidSchemaUpdateRequest;
    switch (predicate.op) {
        .is_null, .is_not_null => if (predicate.value_json != null) return error.InvalidSchemaUpdateRequest,
        .eq, .ne => if (predicate.value_json == null) return error.InvalidSchemaUpdateRequest,
    }
}

fn validateRelationalUniquePredicateExpression(
    schema: TableSchema,
    condition: storage_schema.RelationalRowsExpressionCondition,
) !void {
    try validateRelationalRowsExpressionConditionAgainstSchema(schema, condition);
    if (!relationalRowsExpressionDeterministic(condition.lhs)) return error.InvalidSchemaUpdateRequest;
    for (condition.rhs) |rhs| {
        if (!relationalRowsExpressionDeterministic(rhs)) return error.InvalidSchemaUpdateRequest;
    }
}

fn relationalRowsExpressionDeterministic(expression: storage_schema.RelationalRowsExpression) bool {
    if (expression.field_source != .row) return false;
    if (expression.kind == .now or expression.kind == .uuid_v4) return false;
    for (expression.operands) |operand| {
        if (!relationalRowsExpressionDeterministic(operand)) return false;
    }
    for (expression.case_branches) |branch| {
        if (!relationalRowsExpressionConditionDeterministic(branch.when)) return false;
        if (!relationalRowsExpressionDeterministic(branch.then)) return false;
    }
    for (expression.case_else) |case_else| {
        if (!relationalRowsExpressionDeterministic(case_else)) return false;
    }
    return true;
}

fn relationalRowsExpressionConditionDeterministic(condition: storage_schema.RelationalRowsExpressionCondition) bool {
    if (!relationalRowsExpressionDeterministic(condition.lhs)) return false;
    for (condition.rhs) |rhs| {
        if (!relationalRowsExpressionDeterministic(rhs)) return false;
    }
    return true;
}

fn validateRelationalChecks(schema: TableSchema) !void {
    for (schema.checks, 0..) |check, i| {
        if (check.expression) |expression| {
            if (check.field.len != 0 or check.value_json != null or check.collation != null) return error.InvalidSchemaUpdateRequest;
            try validateRelationalRowsExpressionConditionAgainstSchema(schema, expression);
        } else {
            const property = findDocumentProperty(schema.document_schemas[0].properties, check.field) orelse return error.InvalidSchemaUpdateRequest;
            if (!isRelationalStorageProperty(property)) return error.InvalidSchemaUpdateRequest;
            if (check.collation != null and !isRelationalTextLikeProperty(property)) return error.InvalidSchemaUpdateRequest;
            switch (check.op) {
                .is_null, .is_not_null => if (check.value_json != null) return error.InvalidSchemaUpdateRequest,
                .is_distinct, .is_not_distinct, .eq, .ne => {
                    const value_json = check.value_json orelse return error.InvalidSchemaUpdateRequest;
                    const field_type = try relationalRowsExpressionTypeForProperty(property);
                    const value_type = try relationalRowsExpressionTypeForLiteralJson(value_json);
                    if (!relationalRowsExpressionTypesComparable(field_type, value_type)) return error.InvalidSchemaUpdateRequest;
                },
                .gt, .gte, .lt, .lte => {
                    const value_json = check.value_json orelse return error.InvalidSchemaUpdateRequest;
                    const field_type = try relationalRowsExpressionTypeForProperty(property);
                    const value_type = try relationalRowsExpressionTypeForLiteralJson(value_json);
                    if (!relationalRowsExpressionTypesComparable(field_type, value_type)) return error.InvalidSchemaUpdateRequest;
                    if (!relationalRowsExpressionTypeOrderable(field_type) or !relationalRowsExpressionTypeOrderable(value_type)) return error.InvalidSchemaUpdateRequest;
                },
            }
        }
        for (schema.checks[0..i]) |previous| {
            if (std.mem.eql(u8, previous.name, check.name)) return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn validateRelationalRowsExpressionConditionAgainstSchema(
    schema: TableSchema,
    condition: storage_schema.RelationalRowsExpressionCondition,
) anyerror!void {
    switch (condition.op) {
        .is_null, .is_not_null => if (condition.rhs.len != 0) return error.InvalidSchemaUpdateRequest,
        .is_distinct, .is_not_distinct, .eq, .ne, .gt, .gte, .lt, .lte => if (condition.rhs.len != 1) return error.InvalidSchemaUpdateRequest,
    }
    const lhs_type = try relationalRowsExpressionType(schema, condition.lhs);
    if (condition.rhs.len == 0) return;
    const rhs_type = try relationalRowsExpressionType(schema, condition.rhs[0]);
    if (!relationalRowsExpressionTypesComparable(lhs_type, rhs_type)) return error.InvalidSchemaUpdateRequest;
    switch (condition.op) {
        .gt, .gte, .lt, .lte => if (!relationalRowsExpressionTypeOrderable(lhs_type) or !relationalRowsExpressionTypeOrderable(rhs_type)) return error.InvalidSchemaUpdateRequest,
        else => {},
    }
}

fn validateRelationalRowsExpressionAgainstSchema(
    schema: TableSchema,
    expression: storage_schema.RelationalRowsExpression,
) anyerror!void {
    _ = try relationalRowsExpressionType(schema, expression);
}

fn relationalRowsExpressionContainsInterval(expression: storage_schema.RelationalRowsExpression) bool {
    if (expression.kind == .interval_ns or expression.kind == .interval_months) return true;
    for (expression.operands) |operand| {
        if (relationalRowsExpressionContainsInterval(operand)) return true;
    }
    for (expression.case_branches) |branch| {
        if (relationalRowsExpressionContainsInterval(branch.then)) return true;
        if (relationalRowsExpressionContainsInterval(branch.when.lhs)) return true;
        for (branch.when.rhs) |rhs| {
            if (relationalRowsExpressionContainsInterval(rhs)) return true;
        }
    }
    for (expression.case_else) |case_else| {
        if (relationalRowsExpressionContainsInterval(case_else)) return true;
    }
    return false;
}

fn validateRelationalRowsDateBinStrideExpression(
    schema: TableSchema,
    expression: storage_schema.RelationalRowsExpression,
) anyerror!void {
    switch (expression.kind) {
        .interval_ns => {
            if (expression.operands.len != 1) return error.InvalidSchemaUpdateRequest;
            if (relationalRowsExpressionContainsInterval(expression.operands[0])) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[0])) != .numeric) return error.InvalidSchemaUpdateRequest;
        },
        .interval_months => return error.InvalidSchemaUpdateRequest,
        else => {
            if (relationalRowsExpressionContainsInterval(expression)) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression)) != .numeric) return error.InvalidSchemaUpdateRequest;
        },
    }
}

fn relationalRowsExpressionType(
    schema: TableSchema,
    expression: storage_schema.RelationalRowsExpression,
) anyerror!RelationalRowsExpressionType {
    if (expression.field_source != .row) return error.InvalidSchemaUpdateRequest;
    if (expression.kind == .field) {
        const property = findDocumentProperty(schema.document_schemas[0].properties, expression.field) orelse return error.InvalidSchemaUpdateRequest;
        if (!isRelationalStorageProperty(property)) return error.InvalidSchemaUpdateRequest;
        return relationalRowsExpressionTypeForProperty(property);
    }
    if (expression.kind == .value) return relationalRowsExpressionTypeForLiteralJson(expression.value_json);

    for (expression.case_branches) |branch| try validateRelationalRowsExpressionConditionAgainstSchema(schema, branch.when);

    switch (expression.kind) {
        .field, .value => unreachable,
        .now => return .datetime,
        .uuid_v4 => return .text,
        .lower, .upper, .initcap, .trim, .ltrim, .rtrim, .replace, .translate, .reverse, .md5, .soundex, .concat, .concat_ws => {
            for (expression.operands) |operand| {
                if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, operand))) return error.InvalidSchemaUpdateRequest;
            }
            return .text;
        },
        .regexp_replace, .regexp_substr => {
            if (expression.kind == .regexp_replace and expression.operands.len != 3 and expression.operands.len != 4) return error.InvalidSchemaUpdateRequest;
            if (expression.kind == .regexp_substr and expression.operands.len != 2) return error.InvalidSchemaUpdateRequest;
            for (expression.operands) |operand| {
                if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, operand))) return error.InvalidSchemaUpdateRequest;
            }
            return .text;
        },
        .regexp_count, .regexp_instr => {
            if (expression.operands.len != 2) return error.InvalidSchemaUpdateRequest;
            for (expression.operands) |operand| {
                if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, operand))) return error.InvalidSchemaUpdateRequest;
            }
            return .numeric;
        },
        .substring => {
            if (expression.operands.len != 2 and expression.operands.len != 3) return error.InvalidSchemaUpdateRequest;
            if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, expression.operands[0]))) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[1])) != .numeric) return error.InvalidSchemaUpdateRequest;
            if (expression.operands.len == 3 and (try relationalRowsExpressionType(schema, expression.operands[2])) != .numeric) return error.InvalidSchemaUpdateRequest;
            return .text;
        },
        .overlay => {
            if (expression.operands.len != 3 and expression.operands.len != 4) return error.InvalidSchemaUpdateRequest;
            if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, expression.operands[0]))) return error.InvalidSchemaUpdateRequest;
            if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, expression.operands[1]))) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[2])) != .numeric) return error.InvalidSchemaUpdateRequest;
            if (expression.operands.len == 4 and (try relationalRowsExpressionType(schema, expression.operands[3])) != .numeric) return error.InvalidSchemaUpdateRequest;
            return .text;
        },
        .split_part => {
            if (expression.operands.len != 3) return error.InvalidSchemaUpdateRequest;
            if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, expression.operands[0]))) return error.InvalidSchemaUpdateRequest;
            if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, expression.operands[1]))) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[2])) != .numeric) return error.InvalidSchemaUpdateRequest;
            return .text;
        },
        .left, .right, .repeat => {
            if (expression.operands.len != 2) return error.InvalidSchemaUpdateRequest;
            if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, expression.operands[0]))) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[1])) != .numeric) return error.InvalidSchemaUpdateRequest;
            return .text;
        },
        .lpad, .rpad => {
            if (expression.operands.len != 2 and expression.operands.len != 3) return error.InvalidSchemaUpdateRequest;
            if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, expression.operands[0]))) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[1])) != .numeric) return error.InvalidSchemaUpdateRequest;
            if (expression.operands.len == 3 and !relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, expression.operands[2]))) return error.InvalidSchemaUpdateRequest;
            return .text;
        },
        .chr => {
            if (expression.operands.len != 1) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[0])) != .numeric) return error.InvalidSchemaUpdateRequest;
            return .text;
        },
        .length, .octet_length, .bit_length, .ascii => {
            for (expression.operands) |operand| {
                if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, operand))) return error.InvalidSchemaUpdateRequest;
            }
            return .numeric;
        },
        .strpos => {
            if (expression.operands.len != 2) return error.InvalidSchemaUpdateRequest;
            for (expression.operands) |operand| {
                if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, operand))) return error.InvalidSchemaUpdateRequest;
            }
            return .numeric;
        },
        .starts_with, .ends_with, .like, .ilike => {
            for (expression.operands) |operand| {
                if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, operand))) return error.InvalidSchemaUpdateRequest;
            }
            return .boolean;
        },
        .regexp_match => {
            if (expression.operands.len != 2 and expression.operands.len != 3) return error.InvalidSchemaUpdateRequest;
            for (expression.operands[0..2]) |operand| {
                if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, operand))) return error.InvalidSchemaUpdateRequest;
            }
            if (expression.operands.len == 3 and (try relationalRowsExpressionType(schema, expression.operands[2])) != .boolean) return error.InvalidSchemaUpdateRequest;
            return .boolean;
        },
        .bool_and, .bool_or, .bool_not => {
            for (expression.operands) |operand| {
                if ((try relationalRowsExpressionType(schema, operand)) != .boolean) return error.InvalidSchemaUpdateRequest;
            }
            return .boolean;
        },
        .abs, .round, .trunc, .floor, .ceil, .sqrt, .sign, .power, .mul, .div, .mod, .interval_ns, .interval_months => {
            for (expression.operands) |operand| {
                if ((try relationalRowsExpressionType(schema, operand)) != .numeric) return error.InvalidSchemaUpdateRequest;
            }
            return .numeric;
        },
        .add, .sub => {
            var saw_datetime = false;
            for (expression.operands) |operand| {
                const operand_type = try relationalRowsExpressionType(schema, operand);
                if (operand_type == .datetime) saw_datetime = true else if (operand_type != .numeric) return error.InvalidSchemaUpdateRequest;
            }
            return if (saw_datetime) .datetime else .numeric;
        },
        .date_trunc => {
            if (expression.operands.len != 2) return error.InvalidSchemaUpdateRequest;
            if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, expression.operands[0]))) return error.InvalidSchemaUpdateRequest;
            const value_type = try relationalRowsExpressionType(schema, expression.operands[1]);
            if (value_type != .datetime and value_type != .numeric) return error.InvalidSchemaUpdateRequest;
            return .datetime;
        },
        .date_bin => {
            if (expression.operands.len != 3) return error.InvalidSchemaUpdateRequest;
            try validateRelationalRowsDateBinStrideExpression(schema, expression.operands[0]);
            const source_type = try relationalRowsExpressionType(schema, expression.operands[1]);
            if (source_type != .datetime and source_type != .numeric) return error.InvalidSchemaUpdateRequest;
            const origin_type = try relationalRowsExpressionType(schema, expression.operands[2]);
            if (origin_type != .datetime and origin_type != .numeric) return error.InvalidSchemaUpdateRequest;
            return .datetime;
        },
        .date_part => {
            if (expression.operands.len != 2) return error.InvalidSchemaUpdateRequest;
            if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, expression.operands[0]))) return error.InvalidSchemaUpdateRequest;
            const value_type = try relationalRowsExpressionType(schema, expression.operands[1]);
            if (value_type != .datetime and value_type != .numeric) return error.InvalidSchemaUpdateRequest;
            return .numeric;
        },
        .cast => return switch (expression.cast_type orelse return error.InvalidSchemaUpdateRequest) {
            .text => .text,
            .numeric => .numeric,
            .bool => .boolean,
            .datetime => .datetime,
        },
        .json_extract => {
            if (expression.operands.len != 1) return error.InvalidSchemaUpdateRequest;
            const source_type = try relationalRowsExpressionType(schema, expression.operands[0]);
            if (source_type != .json) return error.InvalidSchemaUpdateRequest;
            return if (expression.json_as_text) .text else .json;
        },
        .json_path_exists => {
            if (expression.operands.len != 1 or expression.json_path.len == 0) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[0])) != .json) return error.InvalidSchemaUpdateRequest;
            return .boolean;
        },
        .json_typeof => {
            if (expression.operands.len != 1) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[0])) != .json) return error.InvalidSchemaUpdateRequest;
            return .text;
        },
        .json_array_length => {
            if (expression.operands.len != 1) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[0])) != .json) return error.InvalidSchemaUpdateRequest;
            return .numeric;
        },
        .json_build_object => {
            if (expression.operands.len % 2 != 0) return error.InvalidSchemaUpdateRequest;
            var index: usize = 0;
            while (index < expression.operands.len) : (index += 2) {
                if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, expression.operands[index]))) return error.InvalidSchemaUpdateRequest;
                _ = try relationalRowsExpressionType(schema, expression.operands[index + 1]);
            }
            return .json;
        },
        .to_jsonb => {
            if (expression.operands.len != 1) return error.InvalidSchemaUpdateRequest;
            _ = try relationalRowsExpressionType(schema, expression.operands[0]);
            return .json;
        },
        .array_length => {
            if (expression.operands.len != 1) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[0])) != .array) return error.InvalidSchemaUpdateRequest;
            return .numeric;
        },
        .array_position => {
            if (expression.operands.len != 2) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[0])) != .array) return error.InvalidSchemaUpdateRequest;
            _ = try relationalRowsExpressionType(schema, expression.operands[1]);
            return .numeric;
        },
        .array_positions => {
            if (expression.operands.len != 2) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[0])) != .array) return error.InvalidSchemaUpdateRequest;
            _ = try relationalRowsExpressionType(schema, expression.operands[1]);
            return .array;
        },
        .array_append, .array_prepend, .array_cat, .array_remove => {
            if (expression.operands.len != 2) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[0])) != .array) return error.InvalidSchemaUpdateRequest;
            if (expression.kind == .array_cat and (try relationalRowsExpressionType(schema, expression.operands[1])) != .array) return error.InvalidSchemaUpdateRequest;
            if (expression.kind != .array_cat) _ = try relationalRowsExpressionType(schema, expression.operands[1]);
            return .array;
        },
        .array_replace => {
            if (expression.operands.len != 3) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[0])) != .array) return error.InvalidSchemaUpdateRequest;
            _ = try relationalRowsExpressionType(schema, expression.operands[1]);
            _ = try relationalRowsExpressionType(schema, expression.operands[2]);
            return .array;
        },
        .array_to_string => {
            if (expression.operands.len != 2 and expression.operands.len != 3) return error.InvalidSchemaUpdateRequest;
            if ((try relationalRowsExpressionType(schema, expression.operands[0])) != .array) return error.InvalidSchemaUpdateRequest;
            if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, expression.operands[1]))) return error.InvalidSchemaUpdateRequest;
            if (expression.operands.len == 3 and !relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, expression.operands[2]))) return error.InvalidSchemaUpdateRequest;
            return .text;
        },
        .string_to_array => {
            for (expression.operands) |operand| {
                if (!relationalRowsExpressionTypeTextLike(try relationalRowsExpressionType(schema, operand))) return error.InvalidSchemaUpdateRequest;
            }
            return .array;
        },
        .coalesce, .nullif, .greatest, .least => return try relationalRowsExpressionCommonType(schema, expression.operands),
        .case => {
            if (expression.case_else.len != 1) return error.InvalidSchemaUpdateRequest;
            var candidate = try relationalRowsExpressionType(schema, expression.case_else[0]);
            for (expression.case_branches) |branch| {
                const branch_type = try relationalRowsExpressionType(schema, branch.then);
                if (!relationalRowsExpressionTypesComparable(candidate, branch_type)) return error.InvalidSchemaUpdateRequest;
                if (candidate == .null) candidate = branch_type;
            }
            return candidate;
        },
    }
}

fn relationalRowsExpressionCommonType(schema: TableSchema, expressions: []const storage_schema.RelationalRowsExpression) anyerror!RelationalRowsExpressionType {
    if (expressions.len == 0) return error.InvalidSchemaUpdateRequest;
    var candidate = try relationalRowsExpressionType(schema, expressions[0]);
    for (expressions[1..]) |expression| {
        const expression_type = try relationalRowsExpressionType(schema, expression);
        if (!relationalRowsExpressionTypesComparable(candidate, expression_type)) return error.InvalidSchemaUpdateRequest;
        if (candidate == .null) candidate = expression_type;
    }
    return candidate;
}

fn relationalRowsExpressionTypeForProperty(property: DocumentProperty) !RelationalRowsExpressionType {
    const field_type = property.field_type orelse return error.InvalidSchemaUpdateRequest;
    if (std.mem.eql(u8, field_type, "boolean")) return .boolean;
    if (std.mem.eql(u8, field_type, "datetime")) return .datetime;
    if (std.mem.eql(u8, field_type, "integer") or std.mem.eql(u8, field_type, "numeric") or std.mem.eql(u8, field_type, "number")) return .numeric;
    if (std.mem.eql(u8, field_type, "json") or std.mem.eql(u8, field_type, "object")) return .json;
    if (std.mem.eql(u8, field_type, "array")) return .array;
    if (isRelationalTextLikeProperty(property) or
        std.mem.eql(u8, field_type, "html") or
        std.mem.eql(u8, field_type, "search_as_you_type") or
        std.mem.eql(u8, field_type, "blob") or
        std.mem.eql(u8, field_type, "geoshape") or
        std.mem.eql(u8, field_type, "geopoint"))
    {
        return .text;
    }
    return error.InvalidSchemaUpdateRequest;
}

fn relationalRowsExpressionTypeForLiteralJson(value_json: []const u8) !RelationalRowsExpressionType {
    if (std.mem.eql(u8, value_json, "null")) return .null;
    if (std.mem.eql(u8, value_json, "true") or std.mem.eql(u8, value_json, "false")) return .boolean;
    if (value_json.len == 0) return error.InvalidSchemaUpdateRequest;
    return switch (value_json[0]) {
        '"' => .text,
        '[' => .array,
        '{' => .json,
        '-', '0'...'9' => .numeric,
        else => error.InvalidSchemaUpdateRequest,
    };
}

fn relationalRowsExpressionTypesComparable(lhs: RelationalRowsExpressionType, rhs: RelationalRowsExpressionType) bool {
    if (lhs == .null or rhs == .null) return true;
    if (relationalRowsExpressionTypeTextLike(lhs) and relationalRowsExpressionTypeTextLike(rhs)) return true;
    if ((lhs == .datetime and rhs == .numeric) or (lhs == .numeric and rhs == .datetime)) return true;
    return lhs == rhs;
}

fn relationalRowsExpressionTypeTextLike(expression_type: RelationalRowsExpressionType) bool {
    return expression_type == .text;
}

fn relationalRowsExpressionTypeOrderable(expression_type: RelationalRowsExpressionType) bool {
    return expression_type == .text or expression_type == .numeric or expression_type == .datetime or expression_type == .boolean;
}

fn validateRelationalGeneratedProperty(schema: TableSchema, document_schema: DocumentSchema, property: DocumentProperty) !void {
    const generated = property.generated orelse return;
    if (!isRelationalStorageProperty(property)) return error.InvalidSchemaUpdateRequest;
    if (requiredFieldsContain(document_schema.required_fields, property.name)) return error.InvalidSchemaUpdateRequest;
    switch (generated.op) {
        .lower, .upper, .md5 => {
            const source_name = generated.field orelse return error.InvalidSchemaUpdateRequest;
            if (std.mem.eql(u8, source_name, property.name)) return error.InvalidSchemaUpdateRequest;
            const source = findDocumentProperty(schema.document_schemas[0].properties, source_name) orelse return error.InvalidSchemaUpdateRequest;
            if (!isRelationalTextLikeProperty(source)) return error.InvalidSchemaUpdateRequest;
            if (!isRelationalTextLikeProperty(property)) return error.InvalidSchemaUpdateRequest;
        },
        .concat => {
            if (generated.fields.len == 0) return error.InvalidSchemaUpdateRequest;
            if (!isRelationalTextLikeProperty(property)) return error.InvalidSchemaUpdateRequest;
            for (generated.fields) |source_name| {
                if (std.mem.eql(u8, source_name, property.name)) return error.InvalidSchemaUpdateRequest;
                const source = findDocumentProperty(schema.document_schemas[0].properties, source_name) orelse return error.InvalidSchemaUpdateRequest;
                if (!isRelationalStorageProperty(source)) return error.InvalidSchemaUpdateRequest;
            }
        },
        .concat_ws => {
            if (generated.fields.len == 0) return error.InvalidSchemaUpdateRequest;
            if (!isRelationalTextLikeProperty(property)) return error.InvalidSchemaUpdateRequest;
            for (generated.fields) |source_name| {
                if (std.mem.eql(u8, source_name, property.name)) return error.InvalidSchemaUpdateRequest;
                const source = findDocumentProperty(schema.document_schemas[0].properties, source_name) orelse return error.InvalidSchemaUpdateRequest;
                if (!isRelationalTextLikeProperty(source)) return error.InvalidSchemaUpdateRequest;
            }
        },
        .expression => {
            const expression = generated.expression orelse return error.InvalidSchemaUpdateRequest;
            if (!relationalRowsExpressionDeterministic(expression)) return error.InvalidSchemaUpdateRequest;
            if (relationalRowsExpressionReferencesField(expression, property.name)) return error.InvalidSchemaUpdateRequest;
            try validateRelationalRowsExpressionAgainstSchema(schema, expression);
            if (!relationalGeneratedExpressionTypeMatchesProperty(try relationalRowsExpressionType(schema, expression), property)) return error.InvalidSchemaUpdateRequest;
        },
    }
}

fn relationalRowsExpressionReferencesField(expression: storage_schema.RelationalRowsExpression, field: []const u8) bool {
    if (expression.kind == .field and std.mem.eql(u8, expression.field, field)) return true;
    for (expression.operands) |operand| {
        if (relationalRowsExpressionReferencesField(operand, field)) return true;
    }
    for (expression.case_branches) |branch| {
        if (relationalRowsExpressionConditionReferencesField(branch.when, field)) return true;
        if (relationalRowsExpressionReferencesField(branch.then, field)) return true;
    }
    for (expression.case_else) |case_else| {
        if (relationalRowsExpressionReferencesField(case_else, field)) return true;
    }
    return false;
}

fn relationalRowsExpressionConditionReferencesField(condition: storage_schema.RelationalRowsExpressionCondition, field: []const u8) bool {
    if (relationalRowsExpressionReferencesField(condition.lhs, field)) return true;
    for (condition.rhs) |rhs| {
        if (relationalRowsExpressionReferencesField(rhs, field)) return true;
    }
    return false;
}

fn relationalGeneratedExpressionTypeMatchesProperty(expression_type: RelationalRowsExpressionType, property: DocumentProperty) bool {
    const property_type = relationalRowsExpressionTypeForProperty(property) catch return false;
    return relationalRowsExpressionTypesComparable(property_type, expression_type);
}

fn validateRelationalPartialIndexProperty(schema: TableSchema, property: DocumentProperty) !void {
    _ = schema;
    if (property.antfly_index != null or
        property.index_lifecycle != null or
        property.index_generation != null or
        property.index_name != null or
        property.index_access_method != null or
        property.index_schema_fingerprint != null or
        property.index_include_columns.len != 0 or
        property.index_keys.len != 0 or
        property.index_where.len != 0 or
        property.index_where_expressions.len != 0)
    {
        return error.InvalidSchemaUpdateRequest;
    }
}

fn isRelationalTextLikeProperty(property: DocumentProperty) bool {
    const field_type = property.field_type orelse return false;
    return std.mem.eql(u8, field_type, "keyword") or
        std.mem.eql(u8, field_type, "link") or
        std.mem.eql(u8, field_type, "string") or
        std.mem.eql(u8, field_type, "text");
}

fn uniqueConstraintsEquivalent(a: UniqueConstraint, b: UniqueConstraint) bool {
    if (!stringSlicesEqual(a.columns, b.columns)) return false;
    if (!uniqueExpressionSlicesEqual(a.expressions, b.expressions)) return false;
    if (!stringSlicesEqual(a.include_columns, b.include_columns)) return false;
    if (a.nulls_not_distinct != b.nulls_not_distinct) return false;
    if (a.deferrable != b.deferrable) return false;
    if (a.timing != b.timing) return false;
    return uniquePredicateSlicesEqual(a.where, b.where) and
        relationalRowsExpressionConditionSlicesEqual(a.where_expressions, b.where_expressions);
}

fn uniqueExpressionSlicesEqual(a: []const UniqueExpression, b: []const UniqueExpression) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!uniqueExpressionsEqual(left, right)) return false;
    }
    return true;
}

fn uniqueExpressionsEqual(a: UniqueExpression, b: UniqueExpression) bool {
    if (a.op != b.op) return false;
    if (!std.mem.eql(u8, a.field, b.field)) return false;
    if (a.expression == null and b.expression == null) return true;
    if (a.expression == null or b.expression == null) return false;
    return relationalRowsExpressionsEqual(a.expression.?, b.expression.?);
}

fn uniquePredicateSlicesEqual(a: []const UniquePredicate, b: []const UniquePredicate) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (left.op != right.op) return false;
        if (!std.mem.eql(u8, left.field, right.field)) return false;
        if (!optionalStringsEqual(left.value_json, right.value_json)) return false;
    }
    return true;
}

fn relationalRowsExpressionConditionSlicesEqual(
    a: []const storage_schema.RelationalRowsExpressionCondition,
    b: []const storage_schema.RelationalRowsExpressionCondition,
) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!relationalRowsExpressionConditionsEqual(left, right)) return false;
    }
    return true;
}

fn relationalRowsExpressionConditionsEqual(
    a: storage_schema.RelationalRowsExpressionCondition,
    b: storage_schema.RelationalRowsExpressionCondition,
) bool {
    if (a.op != b.op or a.rhs.len != b.rhs.len) return false;
    if (!relationalRowsExpressionsEqual(a.lhs, b.lhs)) return false;
    for (a.rhs, b.rhs) |left, right| {
        if (!relationalRowsExpressionsEqual(left, right)) return false;
    }
    return true;
}

fn relationalRowsExpressionsEqual(
    a: storage_schema.RelationalRowsExpression,
    b: storage_schema.RelationalRowsExpression,
) bool {
    if (a.kind != b.kind or
        !std.mem.eql(u8, a.field, b.field) or
        a.field_source != b.field_source or
        !std.mem.eql(u8, a.value_json, b.value_json) or
        !std.mem.eql(u8, a.json_path, b.json_path) or
        a.json_as_text != b.json_as_text or
        a.cast_type != b.cast_type or
        a.operands.len != b.operands.len or
        a.case_branches.len != b.case_branches.len or
        a.case_else.len != b.case_else.len)
    {
        return false;
    }
    for (a.operands, b.operands) |left, right| {
        if (!relationalRowsExpressionsEqual(left, right)) return false;
    }
    for (a.case_branches, b.case_branches) |left, right| {
        if (!relationalRowsExpressionConditionsEqual(left.when, right.when)) return false;
        if (!relationalRowsExpressionsEqual(left.then, right.then)) return false;
    }
    for (a.case_else, b.case_else) |left, right| {
        if (!relationalRowsExpressionsEqual(left, right)) return false;
    }
    return true;
}

fn optionalStringsEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

fn stringSlicesContains(values: []const []const u8, value: []const u8) bool {
    for (values) |candidate| {
        if (std.mem.eql(u8, candidate, value)) return true;
    }
    return false;
}

fn isRelationalUniqueConstraintColumn(property: DocumentProperty) bool {
    if (isExplicitJsonProperty(property)) return false;
    return isRelationalStorageProperty(property);
}

fn isRelationalForeignKeyColumn(property: DocumentProperty) bool {
    const field_type = property.field_type orelse return false;
    return std.mem.eql(u8, field_type, "keyword") or
        std.mem.eql(u8, field_type, "link") or
        std.mem.eql(u8, field_type, "string") or
        std.mem.eql(u8, field_type, "text");
}

fn validateRelationalEmbeddedJsonProperty(property: DocumentProperty) !void {
    const has_embedded_document_config = property.embedded_schema != null or property.embedded_dynamic_templates.len > 0;
    if (has_embedded_document_config and !isExplicitJsonProperty(property)) return error.InvalidSchemaUpdateRequest;

    if (property.embedded_schema) |embedded_schema| try validateRelationalEmbeddedJsonProperty(embedded_schema.*);
    if (property.item) |item| try validateRelationalEmbeddedJsonProperty(item.*);
    for (property.properties) |child| try validateRelationalEmbeddedJsonProperty(child);
    for (property.prefix_items) |child| try validateRelationalEmbeddedJsonProperty(child);
    for (property.pattern_properties) |child| try validateRelationalEmbeddedJsonProperty(child.property.*);
    if (property.additional_properties_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
    if (property.unevaluated_properties_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
    if (property.unevaluated_items_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
    for (property.dependent_schemas) |dependent| try validateRelationalEmbeddedJsonProperty(dependent.schema.*);
    for (property.any_of) |child| try validateRelationalEmbeddedJsonProperty(child);
    for (property.one_of) |child| try validateRelationalEmbeddedJsonProperty(child);
    for (property.all_of) |child| try validateRelationalEmbeddedJsonProperty(child);
    if (property.not_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
    if (property.if_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
    if (property.then_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
    if (property.else_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
    if (property.contains_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
}

fn isExplicitJsonProperty(property: DocumentProperty) bool {
    const field_type = property.field_type orelse return false;
    return std.mem.eql(u8, field_type, "json");
}

fn isRelationalStorageProperty(property: DocumentProperty) bool {
    if (property.field_type) |field_type| {
        if (std.mem.eql(u8, field_type, "embedding")) return false;
        if (std.mem.eql(u8, field_type, "keyword") or
            std.mem.eql(u8, field_type, "link") or
            std.mem.eql(u8, field_type, "string") or
            std.mem.eql(u8, field_type, "text") or
            std.mem.eql(u8, field_type, "html") or
            std.mem.eql(u8, field_type, "search_as_you_type") or
            std.mem.eql(u8, field_type, "boolean") or
            std.mem.eql(u8, field_type, "datetime") or
            std.mem.eql(u8, field_type, "integer") or
            std.mem.eql(u8, field_type, "numeric") or
            std.mem.eql(u8, field_type, "number") or
            std.mem.eql(u8, field_type, "geopoint") or
            std.mem.eql(u8, field_type, "geoshape") or
            std.mem.eql(u8, field_type, "blob") or
            std.mem.eql(u8, field_type, "json") or
            std.mem.eql(u8, field_type, "object") or
            std.mem.eql(u8, field_type, "array")) return true;
        return property.integer_only;
    }
    if (property.integer_only) return true;
    if (property.properties.len > 0 or
        property.item != null or
        (property.additional_properties_allowed orelse false) or
        property.additional_properties_schema != null or
        property.pattern_properties.len > 0 or
        property.dynamic_infer_types) return true;
    return property.const_value != null or property.enum_values.len > 0;
}

fn parseDocumentSchemas(alloc: std.mem.Allocator, value: std.json.Value) ![]DocumentSchema {
    const object = value.object;
    const document_schemas = try alloc.alloc(DocumentSchema, object.count());
    var initialized: usize = 0;
    errdefer {
        for (document_schemas[0..initialized]) |*document_schema| document_schema.deinit(alloc);
        alloc.free(document_schemas);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        const schema_value = entry.value_ptr.object.get("schema").?;
        const context: SchemaContext = .{
            .document_root = schema_value.object,
            .scope_schema = schema_value.object,
        };
        const property = try parseAnonymousProperty(alloc, context, schema_value.object);
        defer alloc.destroy(property);
        document_schemas[initialized] = try parseDocumentSchemaFromProperty(alloc, entry.key_ptr.*, property);
        initialized += 1;
    }
    return document_schemas;
}

fn parseDocumentSchemaFromProperty(
    alloc: std.mem.Allocator,
    name: []const u8,
    property: *DocumentProperty,
) !DocumentSchema {
    const document_schema: DocumentSchema = .{
        .name = try alloc.dupe(u8, name),
        .min_properties = property.min_properties,
        .max_properties = property.max_properties,
        .required_fields = property.required_fields,
        .include_in_all_fields = property.include_in_all_fields,
        .properties = property.properties,
        .pattern_properties = property.pattern_properties,
        .additional_properties_allowed = property.additional_properties_allowed,
        .additional_properties_schema = property.additional_properties_schema,
        .dynamic_infer_types = property.dynamic_infer_types,
        .unevaluated_properties_allowed = property.unevaluated_properties_allowed,
        .unevaluated_properties_schema = property.unevaluated_properties_schema,
        .property_names = property.property_names,
        .dependent_required = property.dependent_required,
        .dependent_schemas = property.dependent_schemas,
        .any_of = property.any_of,
        .one_of = property.one_of,
        .all_of = property.all_of,
        .not_schema = property.not_schema,
        .if_schema = property.if_schema,
        .then_schema = property.then_schema,
        .else_schema = property.else_schema,
    };

    property.required_fields = &.{};
    property.include_in_all_fields = &.{};
    property.properties = &.{};
    property.pattern_properties = &.{};
    property.additional_properties_allowed = null;
    property.additional_properties_schema = null;
    property.dynamic_infer_types = false;
    property.unevaluated_properties_allowed = null;
    property.unevaluated_properties_schema = null;
    property.property_names = null;
    property.dependent_required = &.{};
    property.dependent_schemas = &.{};
    property.any_of = &.{};
    property.one_of = &.{};
    property.all_of = &.{};
    property.not_schema = null;
    property.if_schema = null;
    property.then_schema = null;
    property.else_schema = null;
    property.deinit(alloc);
    return document_schema;
}

fn parseDocumentProperties(alloc: std.mem.Allocator, context: SchemaContext, object: std.json.ObjectMap) anyerror![]DocumentProperty {
    const properties = try alloc.alloc(DocumentProperty, object.count());
    var initialized: usize = 0;
    errdefer {
        for (properties[0..initialized]) |*property| property.deinit(alloc);
        alloc.free(properties);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        const property_object = switch (entry.value_ptr.*) {
            .object => |property_object| property_object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        const property = try parseAnonymousProperty(alloc, context, property_object);
        defer alloc.destroy(property);
        alloc.free(property.name);
        property.name = try alloc.dupe(u8, entry.key_ptr.*);
        properties[initialized] = property.*;
        initialized += 1;
    }
    return properties;
}

fn parseAnonymousProperty(alloc: std.mem.Allocator, context: SchemaContext, unresolved_object: std.json.ObjectMap) anyerror!*DocumentProperty {
    const current_context = context.child(unresolved_object);
    if (unresolved_object.get("$ref")) |ref_value| {
        const ref_path = try parseSchemaRefPath(ref_value);
        if (isRootSchemaRef(ref_path)) {
            const property = try parseAnonymousPropertyKeywords(alloc, current_context, unresolved_object);
            property.root_ref = true;
            return property;
        }
        const resolved_property = try parseAnonymousProperty(alloc, current_context, try resolveSchemaRef(current_context, ref_path));
        errdefer {
            resolved_property.deinit(alloc);
            alloc.destroy(resolved_property);
        }
        if (!hasRefSiblings(unresolved_object)) return resolved_property;

        const sibling_property = try parseAnonymousPropertyKeywords(alloc, current_context, unresolved_object);
        errdefer {
            sibling_property.deinit(alloc);
            alloc.destroy(sibling_property);
        }

        const combined = try alloc.create(DocumentProperty);
        errdefer alloc.destroy(combined);
        const all_of = try alloc.alloc(DocumentProperty, 2);
        all_of[0] = resolved_property.*;
        all_of[1] = sibling_property.*;
        alloc.destroy(resolved_property);
        alloc.destroy(sibling_property);
        combined.* = .{
            .name = try alloc.dupe(u8, ""),
            .all_of = all_of,
        };
        return combined;
    }
    return try parseAnonymousPropertyKeywords(alloc, current_context, unresolved_object);
}

fn parseAnonymousPropertyKeywords(alloc: std.mem.Allocator, context: SchemaContext, object: std.json.ObjectMap) anyerror!*DocumentProperty {
    const property = try alloc.create(DocumentProperty);
    errdefer alloc.destroy(property);

    const type_spec = if (object.get("type")) |property_type|
        try parseTypeSpec(alloc, property_type, false)
    else
        ParsedTypeSpec{};
    const field_type = type_spec.field_type;
    errdefer if (field_type) |owned| alloc.free(owned);
    const format = if (object.get("format")) |format_value|
        switch (format_value) {
            .string => |format_string| try alloc.dupe(u8, format_string),
            .null => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    errdefer if (format) |owned| alloc.free(owned);
    const allows_null = type_spec.allows_null or parseNullableFlag(object);
    const const_value = if (object.get("const")) |const_schema_value|
        try stringifyJsonValue(alloc, const_schema_value)
    else
        null;
    errdefer if (const_value) |owned| alloc.free(owned);
    const minimum = if (object.get("minimum")) |minimum_value|
        if (minimum_value == .null) null else parseJsonNumber(minimum_value) catch return error.InvalidSchemaUpdateRequest
    else
        null;
    const maximum = if (object.get("maximum")) |maximum_value|
        if (maximum_value == .null) null else parseJsonNumber(maximum_value) catch return error.InvalidSchemaUpdateRequest
    else
        null;
    const exclusive_minimum = if (object.get("exclusiveMinimum")) |exclusive_minimum_value|
        if (exclusive_minimum_value == .null) null else parseJsonNumber(exclusive_minimum_value) catch return error.InvalidSchemaUpdateRequest
    else
        null;
    const exclusive_maximum = if (object.get("exclusiveMaximum")) |exclusive_maximum_value|
        if (exclusive_maximum_value == .null) null else parseJsonNumber(exclusive_maximum_value) catch return error.InvalidSchemaUpdateRequest
    else
        null;
    const multiple_of = if (object.get("multipleOf")) |multiple_of_value|
        if (multiple_of_value == .null) null else blk: {
            const parsed = parseJsonNumber(multiple_of_value) catch return error.InvalidSchemaUpdateRequest;
            if (parsed <= 0) return error.InvalidSchemaUpdateRequest;
            break :blk parsed;
        }
    else
        null;
    const min_length = if (object.get("minLength")) |min_length_value|
        if (min_length_value == .null) null else std.math.cast(u64, min_length_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const max_length = if (object.get("maxLength")) |max_length_value|
        if (max_length_value == .null) null else std.math.cast(u64, max_length_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const min_properties = if (object.get("minProperties")) |min_properties_value|
        if (min_properties_value == .null) null else std.math.cast(u64, min_properties_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const max_properties = if (object.get("maxProperties")) |max_properties_value|
        if (max_properties_value == .null) null else std.math.cast(u64, max_properties_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const pattern = if (object.get("pattern")) |pattern_value|
        switch (pattern_value) {
            .string => |pattern_string| try alloc.dupe(u8, pattern_string),
            .null => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    errdefer if (pattern) |owned| alloc.free(owned);
    const min_items = if (object.get("minItems")) |min_items_value|
        if (min_items_value == .null) null else std.math.cast(u64, min_items_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const max_items = if (object.get("maxItems")) |max_items_value|
        if (max_items_value == .null) null else std.math.cast(u64, max_items_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const min_contains = if (object.get("minContains")) |min_contains_value|
        if (min_contains_value == .null) null else std.math.cast(u64, min_contains_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const max_contains = if (object.get("maxContains")) |max_contains_value|
        if (max_contains_value == .null) null else std.math.cast(u64, max_contains_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const unique_items = if (object.get("uniqueItems")) |unique_items_value|
        if (unique_items_value == .null) false else unique_items_value.bool
    else
        false;
    const enum_values: [][]const u8 = if (object.get("enum")) |enum_value|
        if (enum_value == .array) try parseEnumValues(alloc, enum_value.array) else &[_][]const u8{}
    else
        &[_][]const u8{};
    errdefer {
        for (enum_values) |enum_entry| alloc.free(enum_entry);
        if (enum_values.len > 0) alloc.free(enum_values);
    }

    const required_fields: [][]const u8 = if (object.get("required")) |required|
        if (required == .array) try parseRequiredFields(alloc, required.array) else &[_][]const u8{}
    else
        &[_][]const u8{};
    errdefer {
        for (required_fields) |field_name| alloc.free(field_name);
        if (required_fields.len > 0) alloc.free(required_fields);
    }
    const antfly_types: [][]const u8 = if (object.get("x-antfly-types")) |types_value|
        if (types_value == .array) try parseRequiredFields(alloc, types_value.array) else &[_][]const u8{}
    else
        &[_][]const u8{};
    errdefer {
        for (antfly_types) |type_name| alloc.free(type_name);
        if (antfly_types.len > 0) alloc.free(antfly_types);
    }
    const sql_column_name = if (object.get("x-antfly-column-name")) |column_name_value|
        switch (column_name_value) {
            .string => |name| if (isSqlColumnAliasIdentifier(name)) try alloc.dupe(u8, name) else return error.InvalidSchemaUpdateRequest,
            .null => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    errdefer if (sql_column_name) |owned| alloc.free(owned);
    const analyzer = if (object.get("x-antfly-analyzer")) |analyzer_value|
        switch (analyzer_value) {
            .string => |name| try alloc.dupe(u8, name),
            .null => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    errdefer if (analyzer) |owned| alloc.free(owned);
    const collation = if (object.get("collation")) |collation_value|
        switch (collation_value) {
            .string => |name| if (name.len > 0) try alloc.dupe(u8, name) else return error.InvalidSchemaUpdateRequest,
            .null => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    errdefer if (collation) |owned| alloc.free(owned);
    const antfly_index = if (object.get("x-antfly-index")) |index_value|
        switch (index_value) {
            .bool => |enabled| enabled,
            .null => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    const index_lifecycle = if (object.get("x-antfly-index-lifecycle")) |lifecycle_value|
        switch (lifecycle_value) {
            .string => |value| RelationalIndexLifecycle.fromString(value) orelse return error.InvalidSchemaUpdateRequest,
            .null => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    const index_generation = if (object.get("x-antfly-index-generation")) |generation_value|
        switch (generation_value) {
            .integer => |value| if (value > 0) @as(u64, @intCast(value)) else return error.InvalidSchemaUpdateRequest,
            .null => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    const index_name = if (object.get("x-antfly-index-name")) |index_name_value|
        switch (index_name_value) {
            .string => |value| try alloc.dupe(u8, value),
            .null => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    errdefer if (index_name) |value| alloc.free(value);
    const index_access_method = if (object.get("x-antfly-index-access-method")) |method_value|
        switch (method_value) {
            .string => |value| storage_schema.RelationalIndexAccessMethod.fromString(value) orelse return error.InvalidSchemaUpdateRequest,
            .null => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    const index_schema_fingerprint = if (object.get("x-antfly-index-schema-fingerprint")) |fingerprint_value|
        switch (fingerprint_value) {
            .string => |value| if (value.len > 0) try alloc.dupe(u8, value) else return error.InvalidSchemaUpdateRequest,
            .null => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    errdefer if (index_schema_fingerprint) |value| alloc.free(value);
    const index_include_columns: [][]const u8 = if (object.get("x-antfly-index-include")) |include_value| switch (include_value) {
        .array => |array| try parseRequiredFields(alloc, array),
        .null => &[_][]const u8{},
        else => return error.InvalidSchemaUpdateRequest,
    } else &[_][]const u8{};
    errdefer {
        for (index_include_columns) |field_name| alloc.free(field_name);
        if (index_include_columns.len > 0) alloc.free(index_include_columns);
    }
    const index_keys: []storage_schema.RelationalIndexKey = if (object.get("x-antfly-index-keys")) |keys_value|
        if (keys_value == .null) &.{} else try parseRelationalIndexKeysAlloc(alloc, keys_value)
    else
        &.{};
    errdefer freeRelationalIndexKeys(alloc, index_keys);
    const cardinality_proof = if (object.get("x-antfly-cardinality-proof")) |proof_value|
        switch (proof_value) {
            .string => |value| storage_schema.RelationalColumnCardinalityProof.fromString(value) orelse return error.InvalidSchemaUpdateRequest,
            .null => storage_schema.RelationalColumnCardinalityProof.none,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        storage_schema.RelationalColumnCardinalityProof.none;
    const include_in_all_fields: [][]const u8 = if (object.get("x-antfly-include-in-all")) |include_value|
        if (include_value == .array) try parseRequiredFields(alloc, include_value.array) else &[_][]const u8{}
    else
        &[_][]const u8{};
    errdefer {
        for (include_in_all_fields) |field_name| alloc.free(field_name);
        if (include_in_all_fields.len > 0) alloc.free(include_in_all_fields);
    }
    if (object.get("default") != null and object.get("x-antfly-default") != null) return error.InvalidSchemaUpdateRequest;
    const default_value = if (object.get("x-antfly-default")) |server_default_value| blk: {
        if (server_default_value == .null) break :blk null;
        break :blk try parseRelationalDefaultValue(alloc, server_default_value);
    } else if (object.get("default")) |literal_default| RelationalDefaultValue{
        .kind = .literal,
        .value_json = try stringifyJsonValue(alloc, literal_default),
    } else null;
    errdefer if (default_value) |owned_default| {
        var mutable_default = owned_default;
        mutable_default.deinit(alloc);
    };
    const on_update_value = if (object.get("x-antfly-on-update")) |server_update_value| blk: {
        if (server_update_value == .null) break :blk null;
        break :blk try parseRelationalDefaultValue(alloc, server_update_value);
    } else null;
    errdefer if (on_update_value) |owned_update| {
        var mutable_update = owned_update;
        mutable_update.deinit(alloc);
    };
    const generated = if (object.get("generated")) |generated_value| blk: {
        if (generated_value == .null) break :blk null;
        break :blk try parseRelationalGeneratedValue(alloc, generated_value);
    } else null;
    errdefer if (generated) |owned_generated| {
        var mutable_generated = owned_generated;
        mutable_generated.deinit(alloc);
    };
    const index_where: []UniquePredicate = if (object.get("x-antfly-index-where")) |where_value|
        if (where_value == .null) &.{} else try parseUniquePredicates(alloc, where_value)
    else
        &.{};
    errdefer {
        for (index_where) |predicate| {
            alloc.free(predicate.field);
            if (predicate.value_json) |value| alloc.free(value);
        }
        if (index_where.len > 0) alloc.free(index_where);
    }
    const index_where_expressions: []storage_schema.RelationalRowsExpressionCondition = if (object.get("x-antfly-index-where-expressions")) |where_value|
        if (where_value == .null) &.{} else try parseRelationalRowsExpressionConditionsAlloc(alloc, where_value)
    else
        &.{};
    errdefer {
        for (index_where_expressions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
        if (index_where_expressions.len > 0) alloc.free(index_where_expressions);
    }
    const embedded_schema = if (object.get("schema")) |schema_value| blk: {
        if (schema_value == .null) break :blk null;
        if (schema_value != .object) return error.InvalidSchemaUpdateRequest;
        const embedded_context: SchemaContext = .{
            .document_root = schema_value.object,
            .scope_schema = schema_value.object,
        };
        break :blk try parseAnonymousProperty(alloc, embedded_context, schema_value.object);
    } else null;
    errdefer if (embedded_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const embedded_dynamic_templates: []DynamicTemplate = if (object.get("dynamic_templates")) |dynamic_templates|
        if (dynamic_templates == .null) &[_]DynamicTemplate{} else try parseDynamicTemplates(alloc, dynamic_templates)
    else
        &[_]DynamicTemplate{};
    errdefer {
        for (embedded_dynamic_templates) |*dynamic_template| dynamic_template.deinit(alloc);
        if (embedded_dynamic_templates.len > 0) alloc.free(embedded_dynamic_templates);
    }
    const prefix_items: []DocumentProperty = if (object.get("prefixItems")) |prefix_items_value|
        if (prefix_items_value == .array) try parsePropertyVariants(alloc, context, prefix_items_value.array) else &[_]DocumentProperty{}
    else
        &[_]DocumentProperty{};
    errdefer {
        for (prefix_items) |prefix_property| {
            var owned = prefix_property;
            owned.deinit(alloc);
        }
        if (prefix_items.len > 0) alloc.free(prefix_items);
    }
    const additional_items_allowed = if (object.get("additionalItems")) |additional_items_value|
        switch (additional_items_value) {
            .bool => |allowed| allowed,
            .null, .object => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    const additional_properties_allowed = if (object.get("additionalProperties")) |additional_properties_value|
        switch (additional_properties_value) {
            .bool => |allowed| allowed,
            .null, .object => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    const additional_properties_schema = if (object.get("additionalProperties")) |additional_properties_value|
        if (additional_properties_value == .object) try parseAnonymousProperty(alloc, context, additional_properties_value.object) else null
    else
        null;
    errdefer if (additional_properties_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const dynamic_infer_types = if (object.get("x-antfly-dynamic-indexing")) |dynamic_indexing_value|
        try parseDynamicIndexingMode(dynamic_indexing_value)
    else
        false;
    const unevaluated_properties_allowed = if (object.get("unevaluatedProperties")) |unevaluated_properties_value|
        switch (unevaluated_properties_value) {
            .bool => |allowed| allowed,
            .null, .object => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    const unevaluated_properties_schema = if (object.get("unevaluatedProperties")) |unevaluated_properties_value|
        if (unevaluated_properties_value == .object) try parseAnonymousProperty(alloc, context, unevaluated_properties_value.object) else null
    else
        null;
    errdefer if (unevaluated_properties_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const pattern_properties = if (object.get("patternProperties")) |pattern_properties_value|
        if (pattern_properties_value == .object) try parsePatternProperties(alloc, context, pattern_properties_value.object) else &[_]PatternProperty{}
    else
        &[_]PatternProperty{};
    errdefer {
        for (pattern_properties) |pattern_property| {
            var owned = pattern_property;
            owned.deinit(alloc);
        }
        if (pattern_properties.len > 0) alloc.free(pattern_properties);
    }

    const child_properties: []DocumentProperty = if (object.get("properties")) |properties_value|
        if (properties_value == .object) try parseDocumentProperties(alloc, context, properties_value.object) else &[_]DocumentProperty{}
    else
        &[_]DocumentProperty{};
    errdefer {
        for (child_properties) |child| {
            var owned = child;
            owned.deinit(alloc);
        }
        if (child_properties.len > 0) alloc.free(child_properties);
    }
    const property_names = if (object.get("propertyNames")) |property_names_value|
        if (property_names_value == .object) try parseAnonymousProperty(alloc, context, property_names_value.object) else null
    else
        null;
    errdefer if (property_names) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const dependent_required = blk: {
        var explicit = if (object.get("dependentRequired")) |dependent_required_value|
            if (dependent_required_value == .object) try parseDependentRequired(alloc, dependent_required_value.object) else &[_]DependentRequired{}
        else
            &[_]DependentRequired{};
        errdefer freeDependentRequiredSlice(alloc, explicit);
        var legacy = if (object.get("dependencies")) |dependencies_value|
            if (dependencies_value == .object) try parseLegacyDependentRequired(alloc, dependencies_value.object) else &[_]DependentRequired{}
        else
            &[_]DependentRequired{};
        errdefer freeDependentRequiredSlice(alloc, legacy);
        const merged = try mergeDependentRequiredSlices(alloc, explicit, legacy);
        explicit = &[_]DependentRequired{};
        legacy = &[_]DependentRequired{};
        break :blk merged;
    };
    errdefer freeDependentRequiredSlice(alloc, dependent_required);
    const dependent_schemas = blk: {
        var explicit = if (object.get("dependentSchemas")) |dependent_schemas_value|
            if (dependent_schemas_value == .object) try parseDependentSchemas(alloc, context, dependent_schemas_value.object) else &[_]DependentSchema{}
        else
            &[_]DependentSchema{};
        errdefer freeDependentSchemaSlice(alloc, explicit);
        var legacy = if (object.get("dependencies")) |dependencies_value|
            if (dependencies_value == .object) try parseLegacyDependentSchemas(alloc, context, dependencies_value.object) else &[_]DependentSchema{}
        else
            &[_]DependentSchema{};
        errdefer freeDependentSchemaSlice(alloc, legacy);
        const merged = try mergeDependentSchemaSlices(alloc, explicit, legacy);
        explicit = &[_]DependentSchema{};
        legacy = &[_]DependentSchema{};
        break :blk merged;
    };
    errdefer freeDependentSchemaSlice(alloc, dependent_schemas);
    const any_of: []DocumentProperty = if (object.get("anyOf")) |any_of_value|
        if (any_of_value == .array) try parsePropertyVariants(alloc, context, any_of_value.array) else &[_]DocumentProperty{}
    else
        &[_]DocumentProperty{};
    errdefer {
        for (any_of) |child| {
            var owned = child;
            owned.deinit(alloc);
        }
        if (any_of.len > 0) alloc.free(any_of);
    }
    const one_of: []DocumentProperty = if (object.get("oneOf")) |one_of_value|
        if (one_of_value == .array) try parsePropertyVariants(alloc, context, one_of_value.array) else &[_]DocumentProperty{}
    else
        &[_]DocumentProperty{};
    errdefer {
        for (one_of) |child| {
            var owned = child;
            owned.deinit(alloc);
        }
        if (one_of.len > 0) alloc.free(one_of);
    }
    const all_of: []DocumentProperty = if (object.get("allOf")) |all_of_value|
        if (all_of_value == .array) try parsePropertyVariants(alloc, context, all_of_value.array) else &[_]DocumentProperty{}
    else
        &[_]DocumentProperty{};
    errdefer {
        for (all_of) |child| {
            var owned = child;
            owned.deinit(alloc);
        }
        if (all_of.len > 0) alloc.free(all_of);
    }
    const not_schema = if (object.get("not")) |not_value|
        if (not_value == .object) try parseAnonymousProperty(alloc, context, not_value.object) else null
    else
        null;
    errdefer if (not_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const if_schema = if (object.get("if")) |if_value|
        if (if_value == .object) try parseAnonymousProperty(alloc, context, if_value.object) else null
    else
        null;
    errdefer if (if_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const then_schema = if (object.get("then")) |then_value|
        if (then_value == .object) try parseAnonymousProperty(alloc, context, then_value.object) else null
    else
        null;
    errdefer if (then_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const else_schema = if (object.get("else")) |else_value|
        if (else_value == .object) try parseAnonymousProperty(alloc, context, else_value.object) else null
    else
        null;
    errdefer if (else_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const contains_schema = if (object.get("contains")) |contains_value|
        if (contains_value == .object) try parseAnonymousProperty(alloc, context, contains_value.object) else null
    else
        null;
    errdefer if (contains_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };

    const item = if (object.get("items")) |items_value|
        if (items_value == .object) try parseAnonymousProperty(alloc, context, items_value.object) else null
    else if (object.get("additionalItems")) |additional_items_value|
        if (additional_items_value == .object) try parseAnonymousProperty(alloc, context, additional_items_value.object) else null
    else
        null;
    errdefer if (item) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const unevaluated_items_allowed = if (object.get("unevaluatedItems")) |unevaluated_items_value|
        switch (unevaluated_items_value) {
            .bool => |allowed| allowed,
            .null, .object => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    const unevaluated_items_schema = if (object.get("unevaluatedItems")) |unevaluated_items_value|
        if (unevaluated_items_value == .object) try parseAnonymousProperty(alloc, context, unevaluated_items_value.object) else null
    else
        null;
    errdefer if (unevaluated_items_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };

    property.* = .{
        .name = try alloc.dupe(u8, ""),
        .root_ref = false,
        .field_type = field_type,
        .antfly_types = antfly_types,
        .sql_column_name = sql_column_name,
        .analyzer = analyzer,
        .collation = collation,
        .antfly_index = antfly_index,
        .index_lifecycle = index_lifecycle,
        .index_generation = index_generation,
        .index_name = index_name,
        .index_access_method = index_access_method,
        .index_schema_fingerprint = index_schema_fingerprint,
        .index_include_columns = index_include_columns,
        .index_keys = index_keys,
        .cardinality_proof = cardinality_proof,
        .integer_only = type_spec.integer_only,
        .format = format,
        .allows_null = allows_null,
        .const_value = const_value,
        .minimum = minimum,
        .maximum = maximum,
        .exclusive_minimum = exclusive_minimum,
        .exclusive_maximum = exclusive_maximum,
        .multiple_of = multiple_of,
        .min_length = min_length,
        .max_length = max_length,
        .min_properties = min_properties,
        .max_properties = max_properties,
        .pattern = pattern,
        .min_items = min_items,
        .max_items = max_items,
        .additional_items_allowed = additional_items_allowed,
        .min_contains = min_contains,
        .max_contains = max_contains,
        .unique_items = unique_items,
        .enum_values = enum_values,
        .required_fields = required_fields,
        .include_in_all_fields = include_in_all_fields,
        .prefix_items = prefix_items,
        .properties = child_properties,
        .pattern_properties = pattern_properties,
        .additional_properties_allowed = additional_properties_allowed,
        .additional_properties_schema = additional_properties_schema,
        .dynamic_infer_types = dynamic_infer_types,
        .unevaluated_properties_allowed = unevaluated_properties_allowed,
        .unevaluated_properties_schema = unevaluated_properties_schema,
        .property_names = property_names,
        .dependent_required = dependent_required,
        .dependent_schemas = dependent_schemas,
        .any_of = any_of,
        .one_of = one_of,
        .all_of = all_of,
        .not_schema = not_schema,
        .if_schema = if_schema,
        .then_schema = then_schema,
        .else_schema = else_schema,
        .contains_schema = contains_schema,
        .item = item,
        .unevaluated_items_allowed = unevaluated_items_allowed,
        .unevaluated_items_schema = unevaluated_items_schema,
        .embedded_schema = embedded_schema,
        .embedded_dynamic_templates = embedded_dynamic_templates,
        .default_value = default_value,
        .on_update_value = on_update_value,
        .generated = generated,
        .index_where = index_where,
        .index_where_expressions = index_where_expressions,
    };
    if (property.dynamic_infer_types and (!(property.additional_properties_allowed orelse false) or property.additional_properties_schema != null)) {
        return error.InvalidSchemaUpdateRequest;
    }
    return property;
}

fn parseDynamicIndexingMode(value: std.json.Value) !bool {
    if (value != .object) return error.InvalidSchemaUpdateRequest;
    const mode_value = value.object.get("mode") orelse return error.InvalidSchemaUpdateRequest;
    return switch (mode_value) {
        .string => |mode| if (std.mem.eql(u8, mode, "infer_types")) true else error.InvalidSchemaUpdateRequest,
        else => error.InvalidSchemaUpdateRequest,
    };
}

fn parseDependentRequired(alloc: std.mem.Allocator, object: std.json.ObjectMap) ![]DependentRequired {
    const dependencies = try alloc.alloc(DependentRequired, object.count());
    var initialized: usize = 0;
    errdefer {
        for (dependencies[0..initialized]) |*dependency| dependency.deinit(alloc);
        alloc.free(dependencies);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        const required = switch (entry.value_ptr.*) {
            .array => |required| required,
            else => return error.InvalidSchemaUpdateRequest,
        };
        dependencies[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .required_fields = try parseRequiredFields(alloc, required),
        };
        initialized += 1;
    }
    return dependencies;
}

fn parseDependentSchemas(alloc: std.mem.Allocator, context: SchemaContext, object: std.json.ObjectMap) ![]DependentSchema {
    const dependencies = try alloc.alloc(DependentSchema, object.count());
    var initialized: usize = 0;
    errdefer {
        for (dependencies[0..initialized]) |*dependency| dependency.deinit(alloc);
        alloc.free(dependencies);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        const schema_object = switch (entry.value_ptr.*) {
            .object => |schema_object| schema_object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        dependencies[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .schema = try parseAnonymousProperty(alloc, context, schema_object),
        };
        initialized += 1;
    }
    return dependencies;
}

fn parseLegacyDependentRequired(alloc: std.mem.Allocator, object: std.json.ObjectMap) ![]DependentRequired {
    const dependencies = try alloc.alloc(DependentRequired, object.count());
    var initialized: usize = 0;
    errdefer {
        for (dependencies[0..initialized]) |*dependency| dependency.deinit(alloc);
        alloc.free(dependencies);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .array) continue;
        dependencies[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .required_fields = try parseRequiredFields(alloc, entry.value_ptr.array),
        };
        initialized += 1;
    }

    if (initialized == 0) {
        alloc.free(dependencies);
        return &[_]DependentRequired{};
    }
    if (initialized == dependencies.len) return dependencies;

    const trimmed = try alloc.alloc(DependentRequired, initialized);
    @memcpy(trimmed, dependencies[0..initialized]);
    alloc.free(dependencies);
    return trimmed;
}

fn parseLegacyDependentSchemas(alloc: std.mem.Allocator, context: SchemaContext, object: std.json.ObjectMap) ![]DependentSchema {
    const dependencies = try alloc.alloc(DependentSchema, object.count());
    var initialized: usize = 0;
    errdefer {
        for (dependencies[0..initialized]) |*dependency| dependency.deinit(alloc);
        alloc.free(dependencies);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        dependencies[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .schema = try parseAnonymousProperty(alloc, context, entry.value_ptr.object),
        };
        initialized += 1;
    }

    if (initialized == 0) {
        alloc.free(dependencies);
        return &[_]DependentSchema{};
    }
    if (initialized == dependencies.len) return dependencies;

    const trimmed = try alloc.alloc(DependentSchema, initialized);
    @memcpy(trimmed, dependencies[0..initialized]);
    alloc.free(dependencies);
    return trimmed;
}

fn freeDependentRequiredSlice(alloc: std.mem.Allocator, dependencies: []const DependentRequired) void {
    for (dependencies) |dependency| {
        var owned = dependency;
        owned.deinit(alloc);
    }
    if (dependencies.len > 0) alloc.free(dependencies);
}

fn freeDependentSchemaSlice(alloc: std.mem.Allocator, dependencies: []const DependentSchema) void {
    for (dependencies) |dependency| {
        var owned = dependency;
        owned.deinit(alloc);
    }
    if (dependencies.len > 0) alloc.free(dependencies);
}

fn mergeDependentRequiredSlices(
    alloc: std.mem.Allocator,
    primary: []const DependentRequired,
    legacy: []const DependentRequired,
) ![]const DependentRequired {
    if (primary.len == 0) return legacy;
    if (legacy.len == 0) return primary;

    const merged = try alloc.alloc(DependentRequired, primary.len + legacy.len);
    @memcpy(merged[0..primary.len], primary);
    @memcpy(merged[primary.len..], legacy);
    alloc.free(primary);
    alloc.free(legacy);
    return merged;
}

fn mergeDependentSchemaSlices(
    alloc: std.mem.Allocator,
    primary: []const DependentSchema,
    legacy: []const DependentSchema,
) ![]const DependentSchema {
    if (primary.len == 0) return legacy;
    if (legacy.len == 0) return primary;

    const merged = try alloc.alloc(DependentSchema, primary.len + legacy.len);
    @memcpy(merged[0..primary.len], primary);
    @memcpy(merged[primary.len..], legacy);
    alloc.free(primary);
    alloc.free(legacy);
    return merged;
}

fn parseTypeSpec(alloc: std.mem.Allocator, value: std.json.Value, require_object_only: bool) !ParsedTypeSpec {
    const validated = try validateTypeSpecDefinition(value, require_object_only);
    return .{
        .field_type = if (validated.field_type) |field_type| try alloc.dupe(u8, field_type) else null,
        .integer_only = validated.integer_only,
        .allows_null = validated.allows_null,
    };
}

fn parseNullableFlag(object: std.json.ObjectMap) bool {
    return if (object.get("nullable")) |nullable|
        switch (nullable) {
            .bool => |enabled| enabled,
            .null => false,
            else => false,
        }
    else
        false;
}

fn parsePatternProperties(alloc: std.mem.Allocator, context: SchemaContext, object: std.json.ObjectMap) ![]PatternProperty {
    const properties = try alloc.alloc(PatternProperty, object.count());
    var initialized: usize = 0;
    errdefer {
        for (properties[0..initialized]) |*property| property.deinit(alloc);
        alloc.free(properties);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        const property_object = switch (entry.value_ptr.*) {
            .object => |property_object| property_object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        properties[initialized] = .{
            .pattern = try alloc.dupe(u8, entry.key_ptr.*),
            .property = try parseAnonymousProperty(alloc, context, property_object),
        };
        initialized += 1;
    }
    return properties;
}

fn parseRequiredFields(alloc: std.mem.Allocator, required: std.json.Array) ![][]const u8 {
    const fields = try alloc.alloc([]const u8, required.items.len);
    var initialized: usize = 0;
    errdefer {
        for (fields[0..initialized]) |field_name| alloc.free(field_name);
        alloc.free(fields);
    }

    for (required.items) |entry| {
        fields[initialized] = try alloc.dupe(u8, entry.string);
        initialized += 1;
    }
    return fields;
}

fn parseEnumValues(alloc: std.mem.Allocator, values: std.json.Array) ![][]const u8 {
    const enum_values = try alloc.alloc([]const u8, values.items.len);
    var initialized: usize = 0;
    errdefer {
        for (enum_values[0..initialized]) |enum_value| alloc.free(enum_value);
        alloc.free(enum_values);
    }

    for (values.items) |value| {
        enum_values[initialized] = try stringifyJsonValue(alloc, value);
        initialized += 1;
    }
    return enum_values;
}

fn parsePropertyVariants(alloc: std.mem.Allocator, context: SchemaContext, variants: std.json.Array) ![]DocumentProperty {
    const parsed = try alloc.alloc(DocumentProperty, variants.items.len);
    var initialized: usize = 0;
    errdefer {
        for (parsed[0..initialized]) |*variant| variant.deinit(alloc);
        alloc.free(parsed);
    }

    for (variants.items) |variant_value| {
        const variant_object = switch (variant_value) {
            .object => |object| object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        const variant = try parseAnonymousProperty(alloc, context, variant_object);
        defer alloc.destroy(variant);
        parsed[initialized] = variant.*;
        initialized += 1;
    }
    return parsed;
}

fn parseDynamicTemplates(alloc: std.mem.Allocator, value: std.json.Value) ![]DynamicTemplate {
    return switch (value) {
        .object => |object| blk: {
            const templates = try alloc.alloc(DynamicTemplate, object.count());
            var initialized: usize = 0;
            errdefer {
                for (templates[0..initialized]) |*template| template.deinit(alloc);
                alloc.free(templates);
            }

            var it = object.iterator();
            while (it.next()) |entry| {
                templates[initialized] = try parseDynamicTemplate(alloc, entry.key_ptr.*, entry.value_ptr.*);
                initialized += 1;
            }
            break :blk templates;
        },
        .array => |array| blk: {
            const templates = try alloc.alloc(DynamicTemplate, array.items.len);
            var initialized: usize = 0;
            errdefer {
                for (templates[0..initialized]) |*template| template.deinit(alloc);
                alloc.free(templates);
            }

            for (array.items) |item| {
                const name = if (item.object.get("name")) |name| switch (name) {
                    .string => |string| string,
                    else => "",
                } else "";
                templates[initialized] = try parseDynamicTemplate(alloc, name, item);
                initialized += 1;
            }
            break :blk templates;
        },
        else => unreachable,
    };
}

fn parseDynamicTemplate(alloc: std.mem.Allocator, default_name: []const u8, value: std.json.Value) !DynamicTemplate {
    const object = value.object;
    const mapping = object.get("mapping").?.object;
    const field_type = if (mapping.get("type")) |mapping_type|
        switch (mapping_type) {
            .string => |name| try alloc.dupe(u8, name),
            .null => null,
            else => null,
        }
    else
        null;
    errdefer if (field_type) |owned| alloc.free(owned);
    const analyzer = if (mapping.get("analyzer")) |analyzer_value|
        switch (analyzer_value) {
            .string => |name| try alloc.dupe(u8, name),
            .null => null,
            else => null,
        }
    else
        null;
    errdefer if (analyzer) |owned| alloc.free(owned);
    const match_mapping_type = if (object.get("match_mapping_type")) |match_mapping_type_value|
        switch (match_mapping_type_value) {
            .string => |name| try alloc.dupe(u8, name),
            .null => null,
            else => null,
        }
    else
        null;
    errdefer if (match_mapping_type) |owned| alloc.free(owned);

    return .{
        .name = try alloc.dupe(u8, default_name),
        .match_pattern = if (object.get("match")) |match| switch (match) {
            .string => |pattern| try alloc.dupe(u8, pattern),
            else => null,
        } else if (object.get("match_pattern")) |match_pattern| switch (match_pattern) {
            .string => |pattern| try alloc.dupe(u8, pattern),
            else => null,
        } else null,
        .unmatch_pattern = if (object.get("unmatch")) |unmatch| switch (unmatch) {
            .string => |pattern| try alloc.dupe(u8, pattern),
            else => null,
        } else null,
        .path_match = if (object.get("path_match")) |path_match| switch (path_match) {
            .string => |pattern| try alloc.dupe(u8, pattern),
            else => null,
        } else null,
        .path_unmatch = if (object.get("path_unmatch")) |path_unmatch| switch (path_unmatch) {
            .string => |pattern| try alloc.dupe(u8, pattern),
            else => null,
        } else null,
        .match_mapping_type = match_mapping_type,
        .field_type = field_type,
        .analyzer = analyzer,
        .do_index = if (mapping.get("index")) |index| switch (index) {
            .bool => |enabled| enabled,
            else => null,
        } else null,
        .store = if (mapping.get("store")) |store| switch (store) {
            .bool => |enabled| enabled,
            else => null,
        } else null,
        .doc_values = if (mapping.get("doc_values")) |doc_values| switch (doc_values) {
            .bool => |enabled| enabled,
            else => null,
        } else null,
        .include_in_all = if (mapping.get("include_in_all")) |include_in_all| switch (include_in_all) {
            .bool => |enabled| enabled,
            else => null,
        } else null,
    };
}

fn parseForeignKeys(alloc: std.mem.Allocator, value: std.json.Value) ![]ForeignKey {
    const array = value.array;
    const foreign_keys = try alloc.alloc(ForeignKey, array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (foreign_keys[0..initialized]) |*foreign_key| foreign_key.deinit(alloc);
        alloc.free(foreign_keys);
    }

    for (array.items) |item| {
        const object = item.object;
        const references = object.get("references").?.object;
        const timing_clause = if (object.get("timing")) |timing_value|
            foreignKeyTimingClauseFromString(timing_value.string).?
        else
            null;
        const explicit_timing = if (timing_clause) |clause| clause.timing else null;
        const deferrability = if (object.get("deferrable")) |deferrable_value|
            foreignKeyDeferrabilityFromValue(deferrable_value).?
        else
            null;
        const timing_from_deferrability = if (deferrability) |clause| clause.timing else null;
        if (explicit_timing != null and timing_from_deferrability != null and explicit_timing.? != timing_from_deferrability.?) {
            return error.InvalidSchemaUpdateRequest;
        }
        const explicit_deferrable = if (timing_clause) |clause| clause.deferrable else null;
        const deferrable_from_deferrability = if (deferrability) |clause| clause.deferrable else null;
        if (explicit_deferrable != null and deferrable_from_deferrability != null and explicit_deferrable.? != deferrable_from_deferrability.?) {
            return error.InvalidSchemaUpdateRequest;
        }
        const timing = explicit_timing orelse timing_from_deferrability orelse ForeignKeyTiming.immediate;
        const deferrable = explicit_deferrable orelse deferrable_from_deferrability orelse (timing == .deferred);
        foreign_keys[initialized] = .{
            .name = try alloc.dupe(u8, object.get("name").?.string),
            .columns = try parseStringArrayAlloc(alloc, object.get("columns").?),
            .period = if (object.get("period")) |period| try alloc.dupe(u8, period.string) else null,
            .references = .{
                .table = try alloc.dupe(u8, references.get("table").?.string),
                .columns = try parseStringArrayAlloc(alloc, references.get("columns").?),
                .period = if (references.get("period")) |period| try alloc.dupe(u8, period.string) else null,
            },
            .on_delete = if (object.get("on_delete")) |on_delete|
                ForeignKeyAction.fromString(on_delete.string).?
            else
                .restrict,
            .on_update = if (object.get("on_update")) |on_update|
                ForeignKeyAction.fromString(on_update.string).?
            else
                .restrict,
            .timing = timing,
            .deferrable = deferrable,
            .match = if (object.get("match")) |match|
                ForeignKeyMatch.fromString(match.string).?
            else
                .simple,
            .validation_state = if (object.get("validation_state")) |validation_state|
                ForeignKeyValidationState.fromString(validation_state.string).?
            else
                .enforced,
        };
        initialized += 1;
    }
    return foreign_keys;
}

fn parsePrimaryKey(alloc: std.mem.Allocator, value: std.json.Value) !PrimaryKey {
    const object = value.object;
    const name = if (object.get("name")) |name| try alloc.dupe(u8, name.string) else null;
    errdefer if (name) |value_name| alloc.free(value_name);
    const columns = try parseStringArrayAlloc(alloc, object.get("columns").?);
    errdefer {
        for (columns) |column| alloc.free(column);
        if (columns.len > 0) alloc.free(columns);
    }
    const include_columns: [][]const u8 = if (object.get("include_columns")) |include_columns_value| try parseStringArrayAlloc(alloc, include_columns_value) else &.{};
    errdefer {
        for (include_columns) |column| alloc.free(column);
        if (include_columns.len > 0) alloc.free(include_columns);
    }
    const without_overlaps_period = if (object.get("without_overlaps_period")) |period| try alloc.dupe(u8, period.string) else null;
    const timing = try constraintTimingMetadataFromObject(object);
    return .{
        .name = name,
        .columns = columns,
        .include_columns = include_columns,
        .without_overlaps_period = without_overlaps_period,
        .deferrable = timing.deferrable,
        .timing = timing.timing,
    };
}

fn parseExternalBaseSource(alloc: std.mem.Allocator, value: std.json.Value) !storage_schema.ExternalBaseSource {
    try validateExternalBaseSource(value);
    const object = value.object;
    const table_id_value = object.get("table_id") orelse return error.InvalidSchemaUpdateRequest;
    const table_id = try alloc.dupe(u8, table_id_value.string);
    errdefer alloc.free(table_id);
    const format = parseExternalBaseFormat(object.get("format").?.string) orelse return error.InvalidSchemaUpdateRequest;
    const source_uri_value = object.get("uri") orelse object.get("source_uri") orelse return error.InvalidSchemaUpdateRequest;
    const source_uri = try alloc.dupe(u8, source_uri_value.string);
    errdefer alloc.free(source_uri);
    const credential_ref = if (object.get("credentials") orelse object.get("credential_ref")) |credentials|
        try parseExternalCredentialRef(alloc, credentials)
    else
        null;
    errdefer if (credential_ref) |credential| {
        alloc.free(credential.ref_id);
        alloc.free(credential.scope);
    };
    const snapshot_mode = if (object.get("snapshot") orelse object.get("snapshot_mode")) |snapshot|
        try parseExternalSnapshotMode(alloc, snapshot)
    else
        storage_schema.ExternalSnapshotMode.current;
    errdefer switch (snapshot_mode) {
        .current => {},
        .snapshot_id => |snapshot_id| alloc.free(snapshot_id),
        .object_version_digest => |digest| alloc.free(digest),
    };
    const schema_fingerprint_value = object.get("schema_fingerprint") orelse return error.InvalidSchemaUpdateRequest;
    const schema_fingerprint = try alloc.dupe(u8, schema_fingerprint_value.string);
    errdefer alloc.free(schema_fingerprint);
    return .{
        .table_id = table_id,
        .format = format,
        .source_uri = source_uri,
        .credential_ref = credential_ref,
        .snapshot_mode = snapshot_mode,
        .schema_fingerprint = schema_fingerprint,
        .write_policy = if (object.get("write_policy")) |write_policy|
            parseExternalWritePolicy(write_policy.string) orelse return error.InvalidSchemaUpdateRequest
        else
            .read_only,
    };
}

fn parseExternalCredentialRef(alloc: std.mem.Allocator, value: std.json.Value) !storage_schema.ExternalCredentialRef {
    try validateExternalCredentialRef(value);
    const object = value.object;
    const ref_value = object.get("ref") orelse object.get("ref_id") orelse return error.InvalidSchemaUpdateRequest;
    const ref_id = try alloc.dupe(u8, ref_value.string);
    errdefer alloc.free(ref_id);
    const scope = if (object.get("scope")) |scope_value|
        try alloc.dupe(u8, scope_value.string)
    else
        try alloc.dupe(u8, "");
    errdefer alloc.free(scope);
    return .{ .ref_id = ref_id, .scope = scope };
}

fn parseExternalSnapshotMode(alloc: std.mem.Allocator, value: std.json.Value) !storage_schema.ExternalSnapshotMode {
    try validateExternalSnapshotMode(value);
    switch (value) {
        .string => return .current,
        .object => |object| {
            const mode = object.get("mode").?.string;
            if (enumTokenEql(mode, "current") or enumTokenEql(mode, "iceberg_current")) return .current;
            if (enumTokenEql(mode, "snapshot_id") or enumTokenEql(mode, "snapshot")) {
                const id = object.get("id") orelse object.get("snapshot_id") orelse return error.InvalidSchemaUpdateRequest;
                return .{ .snapshot_id = try alloc.dupe(u8, id.string) };
            }
            if (enumTokenEql(mode, "object_version_digest") or enumTokenEql(mode, "raw_parquet_digest")) {
                const digest = object.get("digest") orelse object.get("object_version_digest") orelse return error.InvalidSchemaUpdateRequest;
                return .{ .object_version_digest = try alloc.dupe(u8, digest.string) };
            }
            return error.InvalidSchemaUpdateRequest;
        },
        else => return error.InvalidSchemaUpdateRequest,
    }
}

fn parseExternalBaseFormat(text: []const u8) ?storage_schema.ExternalBaseFormat {
    if (enumTokenEql(text, "parquet") or enumTokenEql(text, "parquet_prefix") or enumTokenEql(text, "raw_parquet")) return .parquet;
    if (enumTokenEql(text, "iceberg")) return .iceberg;
    if (enumTokenEql(text, "lance")) return .lance;
    return null;
}

fn parseExternalWritePolicy(text: []const u8) ?storage_schema.ExternalWritePolicy {
    if (enumTokenEql(text, "read_only")) return .read_only;
    if (enumTokenEql(text, "materialized_overlay")) return .materialized_overlay;
    if (enumTokenEql(text, "iceberg_writer")) return .iceberg_writer;
    if (enumTokenEql(text, "lake_native_relational")) return .lake_native_relational;
    return null;
}

fn parseRelationalPeriods(alloc: std.mem.Allocator, value: std.json.Value) ![]RelationalPeriod {
    const array = value.array;
    if (array.items.len == 0) return &.{};
    const out = try alloc.alloc(RelationalPeriod, array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*period| period.deinit(alloc);
        alloc.free(out);
    }
    for (array.items) |item| {
        const object = item.object;
        out[initialized] = .{
            .name = try alloc.dupe(u8, object.get("name").?.string),
            .start_column = try alloc.dupe(u8, object.get("start_column").?.string),
            .end_column = try alloc.dupe(u8, object.get("end_column").?.string),
            .range_type = if (object.get("range_type")) |range_type| parseRelationalPeriodRangeType(range_type.string) orelse return error.InvalidSchemaUpdateRequest else null,
        };
        initialized += 1;
    }
    return out;
}

fn parseRelationalPeriodRangeType(text: []const u8) ?storage_schema.RelationalPeriodRangeType {
    if (enumTokenEql(text, "numrange")) return .numrange;
    if (enumTokenEql(text, "daterange")) return .daterange;
    if (enumTokenEql(text, "tsrange")) return .tsrange;
    if (enumTokenEql(text, "tstzrange")) return .tstzrange;
    return null;
}

fn validateRelationalPeriodRangeType(range_type: storage_schema.RelationalPeriodRangeType, start_property: DocumentProperty, end_property: DocumentProperty) !void {
    const start_type = start_property.field_type orelse return error.InvalidSchemaUpdateRequest;
    const end_type = end_property.field_type orelse return error.InvalidSchemaUpdateRequest;
    switch (range_type) {
        .numrange => {
            if (!relationalPeriodRangeTypeIsNumeric(start_type) or !relationalPeriodRangeTypeIsNumeric(end_type)) return error.InvalidSchemaUpdateRequest;
        },
        .daterange, .tsrange, .tstzrange => {
            if (!std.mem.eql(u8, start_type, "datetime") or !std.mem.eql(u8, end_type, "datetime")) return error.InvalidSchemaUpdateRequest;
        },
    }
}

fn relationalPeriodRangeTypeIsNumeric(field_type: []const u8) bool {
    return std.mem.eql(u8, field_type, "numeric") or
        std.mem.eql(u8, field_type, "number") or
        std.mem.eql(u8, field_type, "integer");
}

fn parseRelationalDefaultValue(alloc: std.mem.Allocator, value: std.json.Value) !RelationalDefaultValue {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const op_value = object.get("op") orelse return error.InvalidSchemaUpdateRequest;
    if (op_value != .string) return error.InvalidSchemaUpdateRequest;
    const op_text = op_value.string;
    if (enumTokenEql(op_text, "now_ns")) {
        return .{ .kind = .now_ns, .value_json = try alloc.dupe(u8, "") };
    }
    if (enumTokenEql(op_text, "current_date_ns")) {
        return .{ .kind = .current_date_ns, .value_json = try alloc.dupe(u8, "") };
    }
    if (enumTokenEql(op_text, "uuid_v4")) {
        return .{ .kind = .uuid_v4, .value_json = try alloc.dupe(u8, "") };
    }
    if (enumTokenEql(op_text, "sequence_next")) {
        const sequence_value = object.get("sequence") orelse return error.InvalidSchemaUpdateRequest;
        if (sequence_value != .string or sequence_value.string.len == 0) return error.InvalidSchemaUpdateRequest;
        const database_value = object.get("database");
        if (database_value) |actual| if (actual != .string or actual.string.len == 0) return error.InvalidSchemaUpdateRequest;
        const schema_value = object.get("schema");
        if (schema_value) |actual| if (actual != .string or actual.string.len == 0) return error.InvalidSchemaUpdateRequest;
        var out: std.Io.Writer.Allocating = .init(alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeByte('{');
        try writer.print("\"sequence\":{f}", .{std.json.fmt(sequence_value.string, .{})});
        if (database_value) |actual| try writer.print(",\"database\":{f}", .{std.json.fmt(actual.string, .{})});
        if (schema_value) |actual| try writer.print(",\"schema\":{f}", .{std.json.fmt(actual.string, .{})});
        try writer.writeByte('}');
        return .{ .kind = .sequence_next, .value_json = try out.toOwnedSlice() };
    }
    if (enumTokenEql(op_text, "scalar_subquery")) {
        const query_value = object.get("query") orelse return error.InvalidSchemaUpdateRequest;
        if (query_value != .object) return error.InvalidSchemaUpdateRequest;
        if (query_value.object.get("kind") != null) return error.InvalidSchemaUpdateRequest;
        var out: std.Io.Writer.Allocating = .init(alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeByte('{');
        try writer.writeAll("\"query\":");
        try std.json.Stringify.value(query_value, .{}, writer);
        try writer.writeByte('}');
        return .{ .kind = .scalar_subquery, .value_json = try out.toOwnedSlice() };
    }
    return error.InvalidSchemaUpdateRequest;
}

fn parseRelationalGeneratedValue(alloc: std.mem.Allocator, value: std.json.Value) !RelationalGeneratedValue {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const op_value = object.get("op");
    if (op_value) |actual| {
        if (actual != .string or !enumTokenEql(actual.string, "expression")) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("field") != null or object.get("fields") != null or object.get("separator") != null) return error.InvalidSchemaUpdateRequest;
    const expression = try parseRelationalRowsExpressionAlloc(alloc, object.get("expression") orelse return error.InvalidSchemaUpdateRequest);
    var expression_transferred = false;
    errdefer if (!expression_transferred) freeRelationalRowsExpression(alloc, expression);
    const separator = try alloc.dupe(u8, "");
    var separator_transferred = false;
    errdefer if (!separator_transferred) alloc.free(separator);
    expression_transferred = true;
    separator_transferred = true;
    return .{
        .op = .expression,
        .separator = separator,
        .expression = expression,
    };
}

fn parseRelationalChecks(alloc: std.mem.Allocator, value: std.json.Value) ![]RelationalCheck {
    const array = value.array;
    if (array.items.len == 0) return &.{};
    const out = try alloc.alloc(RelationalCheck, array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*check| check.deinit(alloc);
        alloc.free(out);
    }
    for (array.items) |item| {
        const object = item.object;
        const name = try alloc.dupe(u8, object.get("name").?.string);
        var name_transferred = false;
        errdefer if (!name_transferred) alloc.free(name);
        const validation_state = if (object.get("validation_state")) |state| try parseRelationalCheckValidationState(state.string) else .enforced;
        if (object.get("expression")) |expression_value| {
            const expression = try parseRelationalRowsExpressionConditionAlloc(alloc, expression_value);
            var expression_transferred = false;
            errdefer if (!expression_transferred) freeRelationalRowsExpressionCondition(alloc, expression);
            const field = try alloc.dupe(u8, "");
            var field_transferred = false;
            errdefer if (!field_transferred) alloc.free(field);
            out[initialized] = .{
                .name = name,
                .field = field,
                .validation_state = validation_state,
                .expression = expression,
            };
            name_transferred = true;
            field_transferred = true;
            expression_transferred = true;
        } else {
            const field = try alloc.dupe(u8, object.get("field").?.string);
            var field_transferred = false;
            errdefer if (!field_transferred) alloc.free(field);
            const value_json = if (object.get("value")) |check_value| try stringifyJsonValue(alloc, check_value) else null;
            var value_transferred = false;
            errdefer if (!value_transferred) if (value_json) |json| alloc.free(json);
            const collation = if (object.get("collation")) |collation_value| try alloc.dupe(u8, collation_value.string) else null;
            var collation_transferred = false;
            errdefer if (!collation_transferred) if (collation) |collation_text| alloc.free(collation_text);
            out[initialized] = .{
                .name = name,
                .field = field,
                .op = try parseRelationalCheckOp(object.get("op").?.string),
                .value_json = value_json,
                .collation = collation,
                .validation_state = validation_state,
            };
            name_transferred = true;
            field_transferred = true;
            value_transferred = true;
            collation_transferred = true;
        }
        initialized += 1;
    }
    return out;
}

fn parseRelationalCheckOp(op_text: []const u8) !RelationalCheckOp {
    if (enumTokenEql(op_text, "is_null")) return .is_null;
    if (enumTokenEql(op_text, "is_not_null")) return .is_not_null;
    if (enumTokenEql(op_text, "is_distinct")) return .is_distinct;
    if (enumTokenEql(op_text, "is_not_distinct")) return .is_not_distinct;
    if (enumTokenEql(op_text, "eq")) return .eq;
    if (enumTokenEql(op_text, "ne")) return .ne;
    if (enumTokenEql(op_text, "gt")) return .gt;
    if (enumTokenEql(op_text, "gte")) return .gte;
    if (enumTokenEql(op_text, "lt")) return .lt;
    if (enumTokenEql(op_text, "lte")) return .lte;
    return error.InvalidSchemaUpdateRequest;
}

fn parseRelationalCheckValidationState(state_text: []const u8) !RelationalCheckValidationState {
    if (enumTokenEql(state_text, "enforced")) return .enforced;
    if (enumTokenEql(state_text, "unvalidated")) return .unvalidated;
    if (enumTokenEql(state_text, "validating")) return .validating;
    if (enumTokenEql(state_text, "invalid")) return .invalid;
    return error.InvalidSchemaUpdateRequest;
}

fn relationalCheckOpToStorage(op: RelationalCheckOp) storage_schema.RelationalCheckOp {
    return switch (op) {
        .is_null => .is_null,
        .is_not_null => .is_not_null,
        .is_distinct => .is_distinct,
        .is_not_distinct => .is_not_distinct,
        .eq => .eq,
        .ne => .ne,
        .gt => .gt,
        .gte => .gte,
        .lt => .lt,
        .lte => .lte,
    };
}

fn validateRelationalRowsExpressionConditionJson(value: std.json.Value) anyerror!void {
    if (value != .object) return error.InvalidSchemaUpdateRequest;
    try requireJsonObjectOnlyKeys(value.object, &.{ "lhs", "op", "rhs" });
    const lhs = value.object.get("lhs") orelse return error.InvalidSchemaUpdateRequest;
    const op = value.object.get("op") orelse return error.InvalidSchemaUpdateRequest;
    if (op != .string) return error.InvalidSchemaUpdateRequest;
    const check_op = try parseRelationalCheckOp(op.string);
    const rhs = value.object.get("rhs");
    switch (check_op) {
        .is_null, .is_not_null => if (rhs != null) return error.InvalidSchemaUpdateRequest,
        .is_distinct, .is_not_distinct, .eq, .ne, .gt, .gte, .lt, .lte => if (rhs == null) return error.InvalidSchemaUpdateRequest,
    }
    try validateRelationalRowsCatalogExpressionJson(lhs);
    if (rhs) |rhs_value| try validateRelationalRowsCatalogExpressionJson(rhs_value);
}

fn validateRelationalRowsExpressionConditionArrayJson(value: std.json.Value) anyerror!void {
    if (value != .array or value.array.items.len == 0) return error.InvalidSchemaUpdateRequest;
    for (value.array.items) |item| try validateRelationalRowsExpressionConditionJson(item);
}

fn validateRelationalRowsExpressionJson(value: std.json.Value) anyerror!void {
    if (value != .object) return error.InvalidSchemaUpdateRequest;
    try requireJsonObjectOnlyKeys(value.object, &.{ "field", "source", "value", "op", "args", "to", "path", "as_text", "cases", "else" });
    const field = value.object.get("field");
    const literal = value.object.get("value");
    const op = value.object.get("op");
    const present_count: u8 = (if (field != null) @as(u8, 1) else 0) + (if (literal != null) @as(u8, 1) else 0) + (if (op != null) @as(u8, 1) else 0);
    if (present_count != 1) return error.InvalidSchemaUpdateRequest;
    if (value.object.get("source")) |source| {
        if (field == null or source != .string or !std.mem.eql(u8, source.string, "row")) return error.InvalidSchemaUpdateRequest;
    }
    if (field) |field_value| {
        if (field_value != .string or field_value.string.len == 0) return error.InvalidSchemaUpdateRequest;
        try requireJsonObjectOnlyKeys(value.object, &.{ "field", "source" });
        return;
    }
    if (literal != null) {
        try requireJsonObjectOnlyKeys(value.object, &.{"value"});
        return;
    }
    if (op.?.string.len == 0) return error.InvalidSchemaUpdateRequest;
    if (std.mem.eql(u8, op.?.string, "case")) {
        try requireJsonObjectOnlyKeys(value.object, &.{ "op", "cases", "else" });
        const cases = value.object.get("cases") orelse return error.InvalidSchemaUpdateRequest;
        if (cases != .array or cases.array.items.len == 0) return error.InvalidSchemaUpdateRequest;
        for (cases.array.items) |branch| {
            if (branch != .object) return error.InvalidSchemaUpdateRequest;
            try requireJsonObjectOnlyKeys(branch.object, &.{ "when", "then" });
            try validateRelationalRowsExpressionConditionJson(branch.object.get("when") orelse return error.InvalidSchemaUpdateRequest);
            try validateRelationalRowsExpressionJson(branch.object.get("then") orelse return error.InvalidSchemaUpdateRequest);
        }
        try validateRelationalRowsExpressionJson(value.object.get("else") orelse return error.InvalidSchemaUpdateRequest);
        return;
    }
    const args = value.object.get("args") orelse return error.InvalidSchemaUpdateRequest;
    if (args != .array) return error.InvalidSchemaUpdateRequest;
    const expression_kind = try parseRelationalRowsExpressionKind(op.?.string);
    switch (expression_kind) {
        .cast => try requireJsonObjectOnlyKeys(value.object, &.{ "op", "args", "to" }),
        .json_extract => try requireJsonObjectOnlyKeys(value.object, &.{ "op", "args", "path", "as_text" }),
        .json_path_exists => try requireJsonObjectOnlyKeys(value.object, &.{ "op", "args", "path" }),
        else => try requireJsonObjectOnlyKeys(value.object, &.{ "op", "args" }),
    }
    try validateRelationalRowsExpressionArity(expression_kind, args.array.items.len);
    if (expression_kind == .cast) {
        const to = value.object.get("to") orelse return error.InvalidSchemaUpdateRequest;
        if (to != .string or parseRelationalRowsExpressionCastType(to.string) == null) return error.InvalidSchemaUpdateRequest;
    }
    if (expression_kind == .json_extract or expression_kind == .json_path_exists) {
        const path = value.object.get("path") orelse return error.InvalidSchemaUpdateRequest;
        if (path != .string or path.string.len == 0) return error.InvalidSchemaUpdateRequest;
        if (expression_kind == .json_extract) if (value.object.get("as_text")) |as_text| if (as_text != .bool) return error.InvalidSchemaUpdateRequest;
    }
    for (args.array.items) |arg| try validateRelationalRowsExpressionJson(arg);
}

fn validateRelationalRowsCatalogExpressionJson(value: std.json.Value) anyerror!void {
    try validateRelationalRowsExpressionJson(value);
    try validateRelationalRowsCatalogExpressionDeterministicJson(value);
}

fn validateRelationalRowsCatalogExpressionDeterministicJson(value: std.json.Value) anyerror!void {
    if (value != .object) return error.InvalidSchemaUpdateRequest;
    if (value.object.get("field") != null or value.object.get("value") != null) return;
    const op = value.object.get("op") orelse return error.InvalidSchemaUpdateRequest;
    if (op != .string) return error.InvalidSchemaUpdateRequest;
    if (std.mem.eql(u8, op.string, "now") or std.mem.eql(u8, op.string, "uuid_v4")) {
        return error.InvalidSchemaUpdateRequest;
    }
    if (std.mem.eql(u8, op.string, "case")) {
        const cases = value.object.get("cases") orelse return error.InvalidSchemaUpdateRequest;
        for (cases.array.items) |branch| {
            try validateRelationalRowsExpressionConditionJson(branch.object.get("when") orelse return error.InvalidSchemaUpdateRequest);
            try validateRelationalRowsCatalogExpressionDeterministicJson(branch.object.get("then") orelse return error.InvalidSchemaUpdateRequest);
        }
        try validateRelationalRowsCatalogExpressionDeterministicJson(value.object.get("else") orelse return error.InvalidSchemaUpdateRequest);
        return;
    }
    const args = value.object.get("args") orelse return error.InvalidSchemaUpdateRequest;
    for (args.array.items) |arg| try validateRelationalRowsCatalogExpressionDeterministicJson(arg);
}

fn parseRelationalRowsExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) anyerror!storage_schema.RelationalRowsExpressionCondition {
    try validateRelationalRowsExpressionConditionJson(value);
    const lhs = try parseRelationalRowsExpressionAlloc(alloc, value.object.get("lhs").?);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeRelationalRowsExpression(alloc, lhs);
    const op = relationalCheckOpToStorage(try parseRelationalCheckOp(value.object.get("op").?.string));
    const rhs = if (value.object.get("rhs")) |rhs_value| blk: {
        const out = try alloc.alloc(storage_schema.RelationalRowsExpression, 1);
        var out_transferred = false;
        errdefer if (!out_transferred) alloc.free(out);
        out[0] = try parseRelationalRowsExpressionAlloc(alloc, rhs_value);
        out_transferred = true;
        break :blk out;
    } else &.{};
    var rhs_transferred = false;
    errdefer if (!rhs_transferred and rhs.len > 0) {
        for (rhs) |expression| freeRelationalRowsExpression(alloc, expression);
        alloc.free(rhs);
    };
    lhs_transferred = true;
    rhs_transferred = true;
    return .{ .lhs = lhs, .op = op, .rhs = rhs };
}

fn parseRelationalRowsExpressionConditionsAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) anyerror![]storage_schema.RelationalRowsExpressionCondition {
    try validateRelationalRowsExpressionConditionArrayJson(value);
    const out = try alloc.alloc(storage_schema.RelationalRowsExpressionCondition, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
        alloc.free(out);
    }
    for (value.array.items) |item| {
        out[initialized] = try parseRelationalRowsExpressionConditionAlloc(alloc, item);
        initialized += 1;
    }
    return out;
}

fn parseRelationalRowsExpressionAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) anyerror!storage_schema.RelationalRowsExpression {
    try validateRelationalRowsExpressionJson(value);
    if (value.object.get("field")) |field| {
        return .{ .kind = .field, .field = try alloc.dupe(u8, field.string) };
    }
    if (value.object.get("value")) |literal| {
        return .{ .kind = .value, .value_json = try stringifyJsonValue(alloc, literal) };
    }

    const op = value.object.get("op").?.string;
    if (std.mem.eql(u8, op, "case")) return try parseRelationalRowsCaseExpressionAlloc(alloc, value);
    if (std.mem.eql(u8, op, "now")) {
        return .{ .kind = .now, .value_json = try alloc.dupe(u8, "0") };
    }
    if (std.mem.eql(u8, op, "uuid_v4")) {
        return .{ .kind = .uuid_v4 };
    }

    const expression_kind = try parseRelationalRowsExpressionKind(op);
    const args = value.object.get("args").?.array.items;
    const operands = try alloc.alloc(storage_schema.RelationalRowsExpression, args.len);
    var initialized: usize = 0;
    errdefer {
        for (operands[0..initialized]) |operand| freeRelationalRowsExpression(alloc, operand);
        if (operands.len > 0) alloc.free(operands);
    }
    for (args) |arg| {
        operands[initialized] = try parseRelationalRowsExpressionAlloc(alloc, arg);
        initialized += 1;
    }
    const cast_type = if (expression_kind == .cast)
        parseRelationalRowsExpressionCastType(value.object.get("to").?.string).?
    else
        null;
    const json_path = if (expression_kind == .json_extract or expression_kind == .json_path_exists)
        try alloc.dupe(u8, value.object.get("path").?.string)
    else
        "";
    var json_path_transferred = false;
    errdefer if (!json_path_transferred and json_path.len > 0) alloc.free(json_path);
    const json_as_text = if (expression_kind == .json_extract and value.object.get("as_text") != null)
        value.object.get("as_text").?.bool
    else
        false;
    json_path_transferred = true;
    return .{
        .kind = expression_kind,
        .operands = operands,
        .cast_type = cast_type,
        .json_path = json_path,
        .json_as_text = json_as_text,
    };
}

fn parseRelationalRowsCaseExpressionAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) anyerror!storage_schema.RelationalRowsExpression {
    const cases = value.object.get("cases").?.array.items;
    const branches = try alloc.alloc(storage_schema.RelationalRowsExpressionCaseBranch, cases.len);
    var initialized: usize = 0;
    errdefer {
        for (branches[0..initialized]) |branch| freeRelationalRowsExpressionCaseBranch(alloc, branch);
        if (branches.len > 0) alloc.free(branches);
    }
    for (cases) |branch_value| {
        const when = try parseRelationalRowsExpressionConditionAlloc(alloc, branch_value.object.get("when").?);
        var when_transferred = false;
        errdefer if (!when_transferred) freeRelationalRowsExpressionCondition(alloc, when);
        const then = try parseRelationalRowsExpressionAlloc(alloc, branch_value.object.get("then").?);
        var then_transferred = false;
        errdefer if (!then_transferred) freeRelationalRowsExpression(alloc, then);
        branches[initialized] = .{ .when = when, .then = then };
        when_transferred = true;
        then_transferred = true;
        initialized += 1;
    }
    const fallback = try alloc.alloc(storage_schema.RelationalRowsExpression, 1);
    var fallback_initialized = false;
    errdefer {
        if (fallback_initialized) freeRelationalRowsExpression(alloc, fallback[0]);
        alloc.free(fallback);
    }
    fallback[0] = try parseRelationalRowsExpressionAlloc(alloc, value.object.get("else").?);
    fallback_initialized = true;
    return .{ .kind = .case, .case_branches = branches, .case_else = fallback };
}

fn parseRelationalRowsExpressionKind(op: []const u8) !storage_schema.RelationalRowsExpressionKind {
    if (std.mem.eql(u8, op, "coalesce")) return .coalesce;
    if (std.mem.eql(u8, op, "uuid_v4")) return .uuid_v4;
    if (std.mem.eql(u8, op, "lower")) return .lower;
    if (std.mem.eql(u8, op, "upper")) return .upper;
    if (std.mem.eql(u8, op, "initcap")) return .initcap;
    if (std.mem.eql(u8, op, "trim") or std.mem.eql(u8, op, "btrim")) return .trim;
    if (std.mem.eql(u8, op, "ltrim")) return .ltrim;
    if (std.mem.eql(u8, op, "rtrim")) return .rtrim;
    if (std.mem.eql(u8, op, "replace")) return .replace;
    if (std.mem.eql(u8, op, "regexp_replace")) return .regexp_replace;
    if (std.mem.eql(u8, op, "regexp_substr")) return .regexp_substr;
    if (std.mem.eql(u8, op, "translate")) return .translate;
    if (std.mem.eql(u8, op, "substring") or std.mem.eql(u8, op, "substr")) return .substring;
    if (std.mem.eql(u8, op, "overlay")) return .overlay;
    if (std.mem.eql(u8, op, "split_part")) return .split_part;
    if (std.mem.eql(u8, op, "strpos")) return .strpos;
    if (std.mem.eql(u8, op, "left")) return .left;
    if (std.mem.eql(u8, op, "right")) return .right;
    if (std.mem.eql(u8, op, "lpad")) return .lpad;
    if (std.mem.eql(u8, op, "rpad")) return .rpad;
    if (std.mem.eql(u8, op, "repeat")) return .repeat;
    if (std.mem.eql(u8, op, "reverse")) return .reverse;
    if (std.mem.eql(u8, op, "starts_with")) return .starts_with;
    if (std.mem.eql(u8, op, "ends_with")) return .ends_with;
    if (std.mem.eql(u8, op, "ascii")) return .ascii;
    if (std.mem.eql(u8, op, "chr")) return .chr;
    if (std.mem.eql(u8, op, "md5")) return .md5;
    if (std.mem.eql(u8, op, "soundex")) return .soundex;
    if (std.mem.eql(u8, op, "like")) return .like;
    if (std.mem.eql(u8, op, "ilike")) return .ilike;
    if (std.mem.eql(u8, op, "regexp_match")) return .regexp_match;
    if (std.mem.eql(u8, op, "regexp_count")) return .regexp_count;
    if (std.mem.eql(u8, op, "regexp_instr")) return .regexp_instr;
    if (std.mem.eql(u8, op, "and") or std.mem.eql(u8, op, "bool_and")) return .bool_and;
    if (std.mem.eql(u8, op, "or") or std.mem.eql(u8, op, "bool_or")) return .bool_or;
    if (std.mem.eql(u8, op, "not") or std.mem.eql(u8, op, "bool_not")) return .bool_not;
    if (std.mem.eql(u8, op, "concat")) return .concat;
    if (std.mem.eql(u8, op, "concat_ws")) return .concat_ws;
    if (std.mem.eql(u8, op, "length")) return .length;
    if (std.mem.eql(u8, op, "octet_length")) return .octet_length;
    if (std.mem.eql(u8, op, "bit_length")) return .bit_length;
    if (std.mem.eql(u8, op, "nullif")) return .nullif;
    if (std.mem.eql(u8, op, "greatest")) return .greatest;
    if (std.mem.eql(u8, op, "least")) return .least;
    if (std.mem.eql(u8, op, "abs")) return .abs;
    if (std.mem.eql(u8, op, "round")) return .round;
    if (std.mem.eql(u8, op, "trunc")) return .trunc;
    if (std.mem.eql(u8, op, "floor")) return .floor;
    if (std.mem.eql(u8, op, "ceil")) return .ceil;
    if (std.mem.eql(u8, op, "sqrt")) return .sqrt;
    if (std.mem.eql(u8, op, "sign")) return .sign;
    if (std.mem.eql(u8, op, "power")) return .power;
    if (std.mem.eql(u8, op, "add")) return .add;
    if (std.mem.eql(u8, op, "sub")) return .sub;
    if (std.mem.eql(u8, op, "mul")) return .mul;
    if (std.mem.eql(u8, op, "div")) return .div;
    if (std.mem.eql(u8, op, "mod")) return .mod;
    if (std.mem.eql(u8, op, "interval_ns")) return .interval_ns;
    if (std.mem.eql(u8, op, "interval_months")) return .interval_months;
    if (std.mem.eql(u8, op, "date_trunc")) return .date_trunc;
    if (std.mem.eql(u8, op, "date_bin")) return .date_bin;
    if (std.mem.eql(u8, op, "date_part") or std.mem.eql(u8, op, "extract")) return .date_part;
    if (std.mem.eql(u8, op, "cast")) return .cast;
    if (std.mem.eql(u8, op, "json_extract")) return .json_extract;
    if (std.mem.eql(u8, op, "json_path_exists")) return .json_path_exists;
    if (std.mem.eql(u8, op, "json_typeof") or std.mem.eql(u8, op, "jsonb_typeof")) return .json_typeof;
    if (std.mem.eql(u8, op, "json_array_length") or std.mem.eql(u8, op, "jsonb_array_length")) return .json_array_length;
    if (std.mem.eql(u8, op, "json_build_object") or std.mem.eql(u8, op, "jsonb_build_object")) return .json_build_object;
    if (std.mem.eql(u8, op, "to_jsonb")) return .to_jsonb;
    if (std.mem.eql(u8, op, "array_length")) return .array_length;
    if (std.mem.eql(u8, op, "array_position")) return .array_position;
    if (std.mem.eql(u8, op, "array_positions")) return .array_positions;
    if (std.mem.eql(u8, op, "array_append")) return .array_append;
    if (std.mem.eql(u8, op, "array_prepend")) return .array_prepend;
    if (std.mem.eql(u8, op, "array_cat")) return .array_cat;
    if (std.mem.eql(u8, op, "array_remove")) return .array_remove;
    if (std.mem.eql(u8, op, "array_replace")) return .array_replace;
    if (std.mem.eql(u8, op, "array_to_string")) return .array_to_string;
    if (std.mem.eql(u8, op, "string_to_array")) return .string_to_array;
    return error.InvalidSchemaUpdateRequest;
}

fn validateRelationalRowsExpressionArity(kind: storage_schema.RelationalRowsExpressionKind, len: usize) !void {
    switch (kind) {
        .now, .uuid_v4 => if (len != 0) return error.InvalidSchemaUpdateRequest,
        .lower, .upper, .initcap, .length, .octet_length, .bit_length, .ascii, .chr, .md5, .soundex, .reverse, .abs, .round, .trunc, .floor, .ceil, .sqrt, .sign, .interval_ns, .interval_months, .date_part, .date_trunc, .cast, .json_extract, .json_path_exists, .json_typeof, .json_array_length, .array_length, .to_jsonb => if (len != 1 and kind != .date_part and kind != .date_trunc) return error.InvalidSchemaUpdateRequest,
        .power => if (len != 2) return error.InvalidSchemaUpdateRequest,
        .date_bin => if (len != 3) return error.InvalidSchemaUpdateRequest,
        .trim, .ltrim, .rtrim => if (len != 1 and len != 2) return error.InvalidSchemaUpdateRequest,
        .replace, .translate, .split_part => if (len != 3) return error.InvalidSchemaUpdateRequest,
        .regexp_replace => if (len != 3 and len != 4) return error.InvalidSchemaUpdateRequest,
        .regexp_substr => if (len != 2) return error.InvalidSchemaUpdateRequest,
        .substring => if (len != 2 and len != 3) return error.InvalidSchemaUpdateRequest,
        .overlay => if (len != 3 and len != 4) return error.InvalidSchemaUpdateRequest,
        .json_build_object => if (len % 2 != 0) return error.InvalidSchemaUpdateRequest,
        .strpos, .left, .right, .repeat, .starts_with, .ends_with, .like, .ilike, .regexp_count, .regexp_instr, .nullif, .sub, .div, .mod, .array_position, .array_positions, .array_append, .array_prepend, .array_cat, .array_remove, .string_to_array => if (len != 2) return error.InvalidSchemaUpdateRequest,
        .regexp_match => if (len != 2 and len != 3) return error.InvalidSchemaUpdateRequest,
        .array_replace => if (len != 3) return error.InvalidSchemaUpdateRequest,
        .array_to_string => if (len != 2 and len != 3) return error.InvalidSchemaUpdateRequest,
        .lpad, .rpad => if (len != 2 and len != 3) return error.InvalidSchemaUpdateRequest,
        .bool_and, .bool_or, .concat_ws => if (len < 2) return error.InvalidSchemaUpdateRequest,
        .bool_not => if (len != 1) return error.InvalidSchemaUpdateRequest,
        .greatest, .least, .add, .mul, .coalesce, .concat => if (len == 0) return error.InvalidSchemaUpdateRequest,
        .case, .field, .value => return error.InvalidSchemaUpdateRequest,
    }
    if ((kind == .date_part or kind == .date_trunc) and len != 2) return error.InvalidSchemaUpdateRequest;
}

fn parseRelationalRowsExpressionCastType(text: []const u8) ?storage_schema.RelationalRowsExpressionCastType {
    if (std.mem.eql(u8, text, "text")) return .text;
    if (std.mem.eql(u8, text, "numeric")) return .numeric;
    if (std.mem.eql(u8, text, "bool") or std.mem.eql(u8, text, "boolean")) return .bool;
    if (std.mem.eql(u8, text, "datetime")) return .datetime;
    return null;
}

fn freeRelationalRowsExpressionCondition(alloc: std.mem.Allocator, condition: storage_schema.RelationalRowsExpressionCondition) void {
    freeRelationalRowsExpression(alloc, condition.lhs);
    for (condition.rhs) |rhs| freeRelationalRowsExpression(alloc, rhs);
    if (condition.rhs.len > 0) alloc.free(condition.rhs);
}

fn requireJsonObjectOnlyKeys(object: std.json.ObjectMap, allowed: []const []const u8) !void {
    var it = object.iterator();
    while (it.next()) |entry| {
        for (allowed) |key| {
            if (std.mem.eql(u8, entry.key_ptr.*, key)) break;
        } else {
            return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn freeRelationalRowsExpressionCaseBranch(alloc: std.mem.Allocator, branch: storage_schema.RelationalRowsExpressionCaseBranch) void {
    freeRelationalRowsExpressionCondition(alloc, branch.when);
    freeRelationalRowsExpression(alloc, branch.then);
}

fn freeRelationalRowsExpression(alloc: std.mem.Allocator, expression: storage_schema.RelationalRowsExpression) void {
    if (expression.field.len > 0) alloc.free(expression.field);
    if (expression.value_json.len > 0) alloc.free(expression.value_json);
    if (expression.json_path.len > 0) alloc.free(expression.json_path);
    for (expression.operands) |operand| freeRelationalRowsExpression(alloc, operand);
    if (expression.operands.len > 0) alloc.free(expression.operands);
    for (expression.case_branches) |branch| freeRelationalRowsExpressionCaseBranch(alloc, branch);
    if (expression.case_branches.len > 0) alloc.free(expression.case_branches);
    for (expression.case_else) |fallback| freeRelationalRowsExpression(alloc, fallback);
    if (expression.case_else.len > 0) alloc.free(expression.case_else);
}

fn parseUniqueConstraints(alloc: std.mem.Allocator, value: std.json.Value) ![]UniqueConstraint {
    const array = value.array;
    const constraints = try alloc.alloc(UniqueConstraint, array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (constraints[0..initialized]) |*constraint| constraint.deinit(alloc);
        alloc.free(constraints);
    }

    for (array.items) |item| {
        const object = item.object;
        const timing = try constraintTimingMetadataFromObject(object);
        constraints[initialized] = .{
            .name = try alloc.dupe(u8, object.get("name").?.string),
            .columns = if (object.get("columns")) |columns| try parseStringArrayAlloc(alloc, columns) else &.{},
            .expressions = if (object.get("expressions")) |expressions| try parseUniqueExpressions(alloc, expressions) else &.{},
            .include_columns = if (object.get("include_columns")) |include_columns| try parseStringArrayAlloc(alloc, include_columns) else &.{},
            .without_overlaps_period = if (object.get("without_overlaps_period")) |period| try alloc.dupe(u8, period.string) else null,
            .nulls_not_distinct = if (object.get("nulls_not_distinct")) |flag| flag.bool else false,
            .deferrable = timing.deferrable,
            .timing = timing.timing,
            .where = if (object.get("where")) |where| try parseUniquePredicates(alloc, where) else &.{},
            .where_expressions = if (object.get("where_expressions")) |where_expressions| try parseRelationalRowsExpressionConditionsAlloc(alloc, where_expressions) else &.{},
            .validation_state = if (object.get("validation_state")) |validation_state|
                UniqueConstraintValidationState.fromString(validation_state.string).?
            else
                .enforced,
        };
        initialized += 1;
    }
    return constraints;
}

fn parseRelationalIndexes(alloc: std.mem.Allocator, value: std.json.Value) ![]storage_schema.RelationalIndex {
    const array = value.array;
    if (array.items.len == 0) return &.{};
    const indexes = try alloc.alloc(storage_schema.RelationalIndex, array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (indexes[0..initialized]) |index| freeParsedRelationalIndex(alloc, index);
        alloc.free(indexes);
    }

    for (array.items) |item| {
        const object = item.object;
        indexes[initialized] = .{
            .name = try alloc.dupe(u8, object.get("name").?.string),
            .owner_kind = relationalIndexOwnerKindFromString(object.get("owner_kind").?.string).?,
            .owner_name = try alloc.dupe(u8, object.get("owner_name").?.string),
            .access_method = storage_schema.RelationalIndexAccessMethod.fromString(object.get("access_method").?.string).?,
            .method_config_json = if (object.get("method_config")) |method_config| try std.json.Stringify.valueAlloc(alloc, method_config, .{}) else null,
            .unique = if (object.get("unique")) |unique| unique.bool else false,
            .columns = if (object.get("columns")) |columns| try parseStringArrayAlloc(alloc, columns) else &.{},
            .expressions = if (object.get("expressions")) |expressions| try parseStorageUniqueExpressions(alloc, expressions) else &.{},
            .include_columns = if (object.get("include_columns")) |include_columns| try parseStringArrayAlloc(alloc, include_columns) else &.{},
            .keys = if (object.get("keys")) |keys| try parseRelationalIndexKeysAlloc(alloc, keys) else &.{},
            .lifecycle = if (object.get("lifecycle")) |lifecycle| storageRelationalIndexLifecycleFromString(lifecycle.string).? else .ready,
            .generation = if (object.get("generation")) |generation| @intCast(generation.integer) else 0,
            .schema_fingerprint = if (object.get("schema_fingerprint")) |fingerprint| try alloc.dupe(u8, fingerprint.string) else null,
            .where = if (object.get("where")) |where| try parseStorageUniquePredicates(alloc, where) else &.{},
            .where_expressions = if (object.get("where_expressions")) |where_expressions| try parseRelationalRowsExpressionConditionsAlloc(alloc, where_expressions) else &.{},
        };
        initialized += 1;
    }
    return indexes;
}

fn parseUniqueExpressions(alloc: std.mem.Allocator, value: std.json.Value) ![]UniqueExpression {
    const array = value.array;
    if (array.items.len == 0) return &.{};
    const out = try alloc.alloc(UniqueExpression, array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |expression| {
            alloc.free(expression.field);
            if (expression.expression) |row_expression| freeRelationalRowsExpression(alloc, row_expression);
        }
        alloc.free(out);
    }
    for (array.items) |item| {
        const object = item.object;
        const op = object.get("op").?.string;
        if (enumTokenEql(op, "expression")) {
            const field = try alloc.dupe(u8, "");
            var field_transferred = false;
            errdefer if (!field_transferred) alloc.free(field);
            const row_expression = try parseRelationalRowsExpressionAlloc(alloc, object.get("expression").?);
            var expression_transferred = false;
            errdefer if (!expression_transferred) freeRelationalRowsExpression(alloc, row_expression);
            out[initialized] = .{
                .op = .expression,
                .field = field,
                .expression = row_expression,
            };
            field_transferred = true;
            expression_transferred = true;
        } else {
            out[initialized] = .{
                .op = if (enumTokenEql(op, "lower")) .lower else if (enumTokenEql(op, "upper")) .upper else if (enumTokenEql(op, "md5")) .md5 else return error.InvalidSchemaUpdateRequest,
                .field = try alloc.dupe(u8, object.get("field").?.string),
            };
        }
        initialized += 1;
    }
    return out;
}

fn parseStorageUniqueExpressions(alloc: std.mem.Allocator, value: std.json.Value) ![]storage_schema.UniqueExpression {
    const array = value.array;
    if (array.items.len == 0) return &.{};
    const out = try alloc.alloc(storage_schema.UniqueExpression, array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |expression| {
            alloc.free(expression.field);
            if (expression.expression) |row_expression| freeRelationalRowsExpression(alloc, row_expression);
        }
        alloc.free(out);
    }
    for (array.items) |item| {
        const object = item.object;
        const op = object.get("op").?.string;
        if (enumTokenEql(op, "expression")) {
            const field = try alloc.dupe(u8, "");
            var field_transferred = false;
            errdefer if (!field_transferred) alloc.free(field);
            const row_expression = try parseRelationalRowsExpressionAlloc(alloc, object.get("expression").?);
            var expression_transferred = false;
            errdefer if (!expression_transferred) freeRelationalRowsExpression(alloc, row_expression);
            out[initialized] = .{
                .op = .expression,
                .field = field,
                .expression = row_expression,
            };
            field_transferred = true;
            expression_transferred = true;
        } else {
            out[initialized] = .{
                .op = if (enumTokenEql(op, "lower")) .lower else if (enumTokenEql(op, "upper")) .upper else if (enumTokenEql(op, "md5")) .md5 else return error.InvalidSchemaUpdateRequest,
                .field = try alloc.dupe(u8, object.get("field").?.string),
            };
        }
        initialized += 1;
    }
    return out;
}

fn parseUniquePredicates(alloc: std.mem.Allocator, value: std.json.Value) ![]UniquePredicate {
    const array = value.object.get("all").?.array;
    const out = try alloc.alloc(UniquePredicate, array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |predicate| {
            alloc.free(predicate.field);
            if (predicate.value_json) |value_json| alloc.free(value_json);
        }
        alloc.free(out);
    }
    for (array.items) |item| {
        const object = item.object;
        const op_text = object.get("op").?.string;
        const op: UniquePredicateOp = if (enumTokenEql(op_text, "is_null"))
            .is_null
        else if (enumTokenEql(op_text, "is_not_null"))
            .is_not_null
        else if (enumTokenEql(op_text, "eq"))
            .eq
        else if (enumTokenEql(op_text, "ne"))
            .ne
        else
            return error.InvalidSchemaUpdateRequest;
        out[initialized] = .{
            .field = try alloc.dupe(u8, object.get("field").?.string),
            .op = op,
            .value_json = if (object.get("value")) |predicate_value| try stringifyJsonValue(alloc, predicate_value) else null,
        };
        initialized += 1;
    }
    return out;
}

fn parseStorageUniquePredicates(alloc: std.mem.Allocator, value: std.json.Value) ![]storage_schema.UniquePredicate {
    const array = value.object.get("all").?.array;
    const out = try alloc.alloc(storage_schema.UniquePredicate, array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |predicate| {
            alloc.free(predicate.field);
            if (predicate.value_json) |value_json| alloc.free(value_json);
        }
        alloc.free(out);
    }
    for (array.items) |item| {
        const object = item.object;
        const op_text = object.get("op").?.string;
        const op: storage_schema.UniquePredicateOp = if (enumTokenEql(op_text, "is_null"))
            .is_null
        else if (enumTokenEql(op_text, "is_not_null"))
            .is_not_null
        else if (enumTokenEql(op_text, "eq"))
            .eq
        else if (enumTokenEql(op_text, "ne"))
            .ne
        else
            return error.InvalidSchemaUpdateRequest;
        out[initialized] = .{
            .field = try alloc.dupe(u8, object.get("field").?.string),
            .op = op,
            .value_json = if (object.get("value")) |predicate_value| try stringifyJsonValue(alloc, predicate_value) else null,
        };
        initialized += 1;
    }
    return out;
}

fn parseStringArrayAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![][]const u8 {
    const array = value.array;
    const out = try alloc.alloc([]const u8, array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
        alloc.free(out);
    }
    for (array.items) |item| {
        out[initialized] = try alloc.dupe(u8, item.string);
        initialized += 1;
    }
    return out;
}

fn relationalIndexKeyDirectionFromString(value: []const u8) ?storage_schema.RelationalIndexKeyDirection {
    if (std.mem.eql(u8, value, "asc")) return .asc;
    if (std.mem.eql(u8, value, "desc")) return .desc;
    return null;
}

fn relationalIndexKeyNullsFromString(value: []const u8) ?storage_schema.RelationalIndexKeyNulls {
    if (std.mem.eql(u8, value, "default")) return .default;
    if (std.mem.eql(u8, value, "first")) return .first;
    if (std.mem.eql(u8, value, "last")) return .last;
    return null;
}

fn parseRelationalIndexKeysAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]storage_schema.RelationalIndexKey {
    const array = value.array;
    if (array.items.len == 0) return error.InvalidSchemaUpdateRequest;
    const out = try alloc.alloc(storage_schema.RelationalIndexKey, array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |key| alloc.free(key.column);
        alloc.free(out);
    }
    for (array.items) |item| {
        const object = item.object;
        out[initialized] = .{
            .column = try alloc.dupe(u8, object.get("column").?.string),
            .direction = if (object.get("direction")) |direction| relationalIndexKeyDirectionFromString(direction.string).? else .asc,
            .nulls = if (object.get("nulls")) |nulls| relationalIndexKeyNullsFromString(nulls.string).? else .default,
        };
        initialized += 1;
    }
    return out;
}

fn resolveDocumentSchema(
    schema: TableSchema,
    root: std.json.ObjectMap,
) !?DocumentSchema {
    if (schema.document_schemas.len == 0) return null;

    if (root.get("_type")) |type_value| {
        if (type_value == .null) return error.InvalidBatchRequest;
        const document_type = switch (type_value) {
            .string => |document_type| document_type,
            else => return error.InvalidBatchRequest,
        };
        return findDocumentSchema(schema.document_schemas, document_type) orelse return error.InvalidBatchRequest;
    }

    if (schema.default_type.len > 0) {
        if (findDocumentSchema(schema.document_schemas, schema.default_type)) |document_schema| return document_schema;
    }
    if (schema.document_schemas.len == 1) return schema.document_schemas[0];
    return error.InvalidBatchRequest;
}

fn findDocumentSchema(document_schemas: []const DocumentSchema, document_type: []const u8) ?DocumentSchema {
    for (document_schemas) |document_schema| {
        if (std.mem.eql(u8, document_schema.name, document_type)) return document_schema;
    }
    return null;
}

fn findDocumentProperty(properties: []const DocumentProperty, field_name: []const u8) ?DocumentProperty {
    for (properties) |property| {
        if (std.mem.eql(u8, property.name, field_name)) return property;
    }
    return null;
}

fn shouldIgnoreSchemaValidationField(field_name: []const u8) bool {
    return field_name.len > 0 and field_name[0] == '_';
}

fn fieldMatchesDynamicTemplates(dynamic_templates: []const DynamicTemplate, path: []const u8, value: std.json.Value) bool {
    const field_name = fieldNameFromPath(path);
    for (dynamic_templates) |dynamic_template| {
        if (dynamicTemplateMatches(dynamic_template, path, field_name, value)) return true;
    }
    return false;
}

fn dynamicTemplateMatches(
    dynamic_template: DynamicTemplate,
    path: []const u8,
    field_name: []const u8,
    value: std.json.Value,
) bool {
    if (dynamic_template.match_pattern) |match_pattern| {
        if (!globMatch(match_pattern, field_name)) return false;
    }
    if (dynamic_template.unmatch_pattern) |unmatch_pattern| {
        if (globMatch(unmatch_pattern, field_name)) return false;
    }
    if (dynamic_template.path_match) |path_match| {
        if (!globMatch(path_match, path)) return false;
    }
    if (dynamic_template.path_unmatch) |path_unmatch| {
        if (globMatch(path_unmatch, path)) return false;
    }
    if (dynamic_template.match_mapping_type) |match_mapping_type| {
        const inferred = inferDynamicTemplateMatchType(value) orelse return false;
        if (!std.mem.eql(u8, match_mapping_type, inferred)) return false;
    }
    return true;
}

fn inferDynamicTemplateMatchType(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |text| if (parseRfc3339ToNs(text) != null or isValidDate(text)) "date" else "string",
        .integer, .float, .number_string => "number",
        .bool => "boolean",
        .object => "object",
        else => null,
    };
}

fn fieldNameFromPath(path: []const u8) []const u8 {
    const last_dot = std.mem.lastIndexOfScalar(u8, path, '.') orelse return path;
    return path[last_dot + 1 ..];
}

fn validateDocumentFieldValueWithContext(
    context: *RuntimeValidationContext,
    property: DocumentProperty,
    value: *const std.json.Value,
    enforce_types: bool,
) !void {
    const composed_enforce_types = false;

    if (property.root_ref) {
        const root_property = context.root_property orelse return error.InvalidBatchRequest;
        if (try context.rootRefGuard(value)) |*guard| {
            defer guard.release();
            try validateDocumentFieldValueWithContext(context, root_property.*, value, enforce_types);
        }
    }

    if (value.* == .null) return validateNullValueWithContext(context, property, value, enforce_types);

    if (property.all_of.len > 0) {
        for (property.all_of) |variant| try validateDocumentFieldValueWithContext(context, variant, value, composed_enforce_types);
    }

    if (property.any_of.len > 0) {
        var matched = false;
        for (property.any_of) |variant| {
            validateDocumentFieldValueWithContext(context, variant, value, composed_enforce_types) catch continue;
            matched = true;
            break;
        }
        if (!matched) return error.InvalidBatchRequest;
    }

    if (property.one_of.len > 0) {
        var matches: usize = 0;
        for (property.one_of) |variant| {
            validateDocumentFieldValueWithContext(context, variant, value, composed_enforce_types) catch continue;
            matches += 1;
        }
        if (matches != 1) return error.InvalidBatchRequest;
    }

    if (property.not_schema) |not_schema| {
        if (validateDocumentFieldValueWithContext(context, not_schema.*, value, composed_enforce_types)) |_| {
            return error.InvalidBatchRequest;
        } else |_| {}
    }

    if (property.if_schema) |if_schema| {
        const matched = if (validateDocumentFieldValueWithContext(context, if_schema.*, value, composed_enforce_types)) |_| true else |_| false;
        if (matched) {
            if (property.then_schema) |then_schema| try validateDocumentFieldValueWithContext(context, then_schema.*, value, composed_enforce_types);
        } else {
            if (property.else_schema) |else_schema| try validateDocumentFieldValueWithContext(context, else_schema.*, value, composed_enforce_types);
        }
    }

    if (property.const_value) |const_value| {
        const rendered = try stringifyJsonValue(std.heap.page_allocator, value.*);
        defer std.heap.page_allocator.free(rendered);
        if (!std.mem.eql(u8, const_value, rendered)) return error.InvalidBatchRequest;
    }

    if (property.enum_values.len > 0) {
        const rendered = try stringifyJsonValue(std.heap.page_allocator, value.*);
        defer std.heap.page_allocator.free(rendered);

        var matched = false;
        for (property.enum_values) |enum_value| {
            if (std.mem.eql(u8, enum_value, rendered)) {
                matched = true;
                break;
            }
        }
        if (!matched) return error.InvalidBatchRequest;
    }

    if (property.pattern) |pattern| {
        const string_value = switch (value.*) {
            .string => |string| string,
            else => return error.InvalidBatchRequest,
        };
        if (!try regexMatch(pattern, string_value)) return error.InvalidBatchRequest;
    }

    if (property.format) |format| {
        const string_value = switch (value.*) {
            .string => |string| string,
            else => return error.InvalidBatchRequest,
        };
        try validateStringFormat(format, string_value);
    }

    if (property.min_length != null or property.max_length != null) {
        if (value.* == .string) {
            const codepoints = std.unicode.utf8CountCodepoints(value.string) catch return error.InvalidBatchRequest;
            if (property.min_length) |min_length| {
                if (codepoints < min_length) return error.InvalidBatchRequest;
            }
            if (property.max_length) |max_length| {
                if (codepoints > max_length) return error.InvalidBatchRequest;
            }
        } else if (property.field_type == null) {
            return error.InvalidBatchRequest;
        }
    }

    if (property.min_items != null or property.max_items != null) {
        if (value.* == .array) {
            if (property.min_items) |min_items| {
                if (value.array.items.len < min_items) return error.InvalidBatchRequest;
            }
            if (property.max_items) |max_items| {
                if (value.array.items.len > max_items) return error.InvalidBatchRequest;
            }
        } else if (property.item == null and property.field_type == null) {
            return error.InvalidBatchRequest;
        }
    }

    if (property.prefix_items.len > 0) {
        const array = switch (value.*) {
            .array => |array| array,
            else => return error.InvalidBatchRequest,
        };
        const prefix_len = @min(property.prefix_items.len, array.items.len);
        for (property.prefix_items[0..prefix_len], array.items[0..prefix_len]) |prefix_item, item_value| {
            try validateDocumentFieldValueWithContext(context, prefix_item, &item_value, enforce_types);
        }
        if (property.additional_items_allowed != null and property.item == null) {
            if (!(property.additional_items_allowed orelse true) and array.items.len > prefix_len) {
                return error.InvalidBatchRequest;
            }
        }
    }

    if (property.unique_items) {
        const array = switch (value.*) {
            .array => |array| array,
            else => return error.InvalidBatchRequest,
        };
        for (array.items, 0..) |item, i| {
            for (array.items[i + 1 ..]) |other| {
                if (jsonValueEqual(item, other)) return error.InvalidBatchRequest;
            }
        }
    }

    if (property.contains_schema != null or property.min_contains != null or property.max_contains != null) {
        const array = switch (value.*) {
            .array => |array| array,
            else => return error.InvalidBatchRequest,
        };
        const contains_schema = property.contains_schema orelse return error.InvalidBatchRequest;
        var match_count: u64 = 0;
        for (array.items) |item_value| {
            validateDocumentFieldValueWithContext(context, contains_schema.*, &item_value, enforce_types) catch continue;
            match_count += 1;
        }
        const min_contains = property.min_contains orelse 1;
        if (match_count < min_contains) return error.InvalidBatchRequest;
        if (property.max_contains) |max_contains| {
            if (match_count > max_contains) return error.InvalidBatchRequest;
        }
    }

    if (property.unevaluated_items_allowed != null or property.unevaluated_items_schema != null) {
        const array = switch (value.*) {
            .array => |array| array,
            else => return error.InvalidBatchRequest,
        };
        var evaluated_indices = std.AutoHashMapUnmanaged(usize, void).empty;
        defer evaluated_indices.deinit(context.alloc);
        try collectEvaluatedArrayIndices(context, property, value, enforce_types, &evaluated_indices, false);
        for (array.items, 0..) |item_value, index| {
            if (evaluated_indices.contains(index)) continue;
            if (property.unevaluated_items_schema) |unevaluated_items_schema| {
                try validateDocumentFieldValueWithContext(context, unevaluated_items_schema.*, &item_value, enforce_types);
                continue;
            }
            if (property.unevaluated_items_allowed) |allowed| {
                if (!allowed) return error.InvalidBatchRequest;
            }
        }
    }

    if (property.properties.len > 0 or
        property.pattern_properties.len > 0 or
        property.required_fields.len > 0 or
        property.additional_properties_allowed != null or
        property.additional_properties_schema != null or
        property.unevaluated_properties_allowed != null or
        property.unevaluated_properties_schema != null or
        property.property_names != null or
        property.dependent_required.len > 0 or
        property.dependent_schemas.len > 0)
    {
        const object = switch (value.*) {
            .object => |object| object,
            else => return error.InvalidBatchRequest,
        };
        try validateObjectCardinality(object, property.min_properties, property.max_properties);
        try validateRequiredFieldsPresent(object, property.required_fields);
        if (property.property_names) |property_names| try validatePropertyNames(context, object, property_names.*, enforce_types);
        try validateDependentRequired(object, property.dependent_required);
        try validateDependentSchemas(context, object, property.dependent_schemas, composed_enforce_types);
        var composition_evaluated_fields = std.StringHashMapUnmanaged(void).empty;
        defer composition_evaluated_fields.deinit(context.alloc);
        try collectComposedObjectFieldCoverage(context, property, object, enforce_types, &composition_evaluated_fields, false);
        var it = object.iterator();
        while (it.next()) |entry| {
            if (shouldIgnoreSchemaValidationField(entry.key_ptr.*)) continue;
            if (findDocumentProperty(property.properties, entry.key_ptr.*)) |child_property| {
                try validateDocumentFieldValueWithContext(context, child_property, entry.value_ptr, enforce_types);
                continue;
            }
            if (try validatePatternProperties(context, entry.key_ptr.*, entry.value_ptr, property.pattern_properties, enforce_types)) continue;
            if (property.additional_properties_schema) |additional_properties_schema| {
                try validateDocumentFieldValueWithContext(context, additional_properties_schema.*, entry.value_ptr, enforce_types);
                continue;
            }
            if (property.additional_properties_allowed) |allowed| {
                if (!allowed) return error.InvalidBatchRequest;
                continue;
            }
            if (composition_evaluated_fields.contains(entry.key_ptr.*)) continue;
            if (property.unevaluated_properties_schema) |unevaluated_properties_schema| {
                try validateDocumentFieldValueWithContext(context, unevaluated_properties_schema.*, entry.value_ptr, enforce_types);
                continue;
            }
            if (property.unevaluated_properties_allowed) |allowed| {
                if (!allowed) return error.InvalidBatchRequest;
                continue;
            }
            if (enforce_types) return error.InvalidBatchRequest;
        }
    }

    if (property.item) |item| {
        const array = switch (value.*) {
            .array => |array| array,
            else => return error.InvalidBatchRequest,
        };
        if (property.min_items) |min_items| {
            if (array.items.len < min_items) return error.InvalidBatchRequest;
        }
        if (property.max_items) |max_items| {
            if (array.items.len > max_items) return error.InvalidBatchRequest;
        }
        const start_index = @min(property.prefix_items.len, array.items.len);
        for (array.items[start_index..]) |item_value| try validateDocumentFieldValueWithContext(context, item.*, &item_value, enforce_types);
    }

    const field_type = property.field_type orelse return;

    if (std.mem.eql(u8, field_type, "text") or
        std.mem.eql(u8, field_type, "keyword") or
        std.mem.eql(u8, field_type, "link") or
        std.mem.eql(u8, field_type, "blob") or
        std.mem.eql(u8, field_type, "html") or
        std.mem.eql(u8, field_type, "search_as_you_type") or
        std.mem.eql(u8, field_type, "string"))
    {
        if (value.* != .string) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, field_type, "numeric") or
        std.mem.eql(u8, field_type, "number") or
        std.mem.eql(u8, field_type, "integer"))
    {
        const numeric_value = parseJsonNumber(value.*) catch return error.InvalidBatchRequest;
        if ((property.integer_only or std.mem.eql(u8, field_type, "integer")) and !isIntegralJsonNumber(value.*, numeric_value)) {
            return error.InvalidBatchRequest;
        }
        if (property.minimum) |minimum| {
            if (numeric_value < minimum) return error.InvalidBatchRequest;
        }
        if (property.maximum) |maximum| {
            if (numeric_value > maximum) return error.InvalidBatchRequest;
        }
        if (property.exclusive_minimum) |exclusive_minimum| {
            if (numeric_value <= exclusive_minimum) return error.InvalidBatchRequest;
        }
        if (property.exclusive_maximum) |exclusive_maximum| {
            if (numeric_value >= exclusive_maximum) return error.InvalidBatchRequest;
        }
        if (property.multiple_of) |multiple_of| {
            if (!isMultipleOf(numeric_value, multiple_of)) return error.InvalidBatchRequest;
        }
        return;
    }
    if (std.mem.eql(u8, field_type, "boolean")) {
        if (value.* != .bool) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, field_type, "null")) return error.InvalidBatchRequest;
    if (std.mem.eql(u8, field_type, "datetime")) {
        switch (value.*) {
            .string, .integer, .number_string => return,
            else => return error.InvalidBatchRequest,
        }
    }
    if (std.mem.eql(u8, field_type, "object")) {
        if (value.* != .object) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, field_type, "array")) {
        const array = switch (value.*) {
            .array => |array| array,
            else => return error.InvalidBatchRequest,
        };
        if (property.min_items) |min_items| {
            if (array.items.len < min_items) return error.InvalidBatchRequest;
        }
        if (property.max_items) |max_items| {
            if (array.items.len > max_items) return error.InvalidBatchRequest;
        }
        return;
    }
}

fn validateNullValueWithContext(
    context: *RuntimeValidationContext,
    property: DocumentProperty,
    value: *const std.json.Value,
    enforce_types: bool,
) !void {
    _ = enforce_types;
    const composed_enforce_types = false;
    const has_all_of = property.all_of.len > 0;

    if (has_all_of) {
        for (property.all_of) |variant| try validateNullValueWithContext(context, variant, value, composed_enforce_types);
    }

    if (property.any_of.len > 0) {
        for (property.any_of) |variant| {
            validateNullValueWithContext(context, variant, value, composed_enforce_types) catch continue;
            return;
        }
        return error.InvalidBatchRequest;
    }

    if (property.one_of.len > 0) {
        var matches: usize = 0;
        for (property.one_of) |variant| {
            validateNullValueWithContext(context, variant, value, composed_enforce_types) catch continue;
            matches += 1;
        }
        if (matches != 1) return error.InvalidBatchRequest;
        return;
    }

    if (property.not_schema) |not_schema| {
        if (validateNullValueWithContext(context, not_schema.*, value, composed_enforce_types)) |_| {
            return error.InvalidBatchRequest;
        } else |_| {}
    }

    if (property.if_schema) |if_schema| {
        const matched = if (validateNullValueWithContext(context, if_schema.*, value, composed_enforce_types)) |_| true else |_| false;
        if (matched) {
            if (property.then_schema) |then_schema| {
                try validateNullValueWithContext(context, then_schema.*, value, composed_enforce_types);
                return;
            }
            return;
        }
        if (property.else_schema) |else_schema| {
            try validateNullValueWithContext(context, else_schema.*, value, composed_enforce_types);
            return;
        }
    }

    if (property.const_value) |const_value| {
        if (!std.mem.eql(u8, const_value, "null")) return error.InvalidBatchRequest;
        return;
    }

    if (property.enum_values.len > 0) {
        for (property.enum_values) |enum_value| {
            if (std.mem.eql(u8, enum_value, "null")) return;
        }
        return error.InvalidBatchRequest;
    }

    if (property.allows_null) return;
    if (property.field_type) |field_type| {
        if (std.mem.eql(u8, field_type, "null")) return;
    }
    if (has_all_of and property.field_type == null and property.const_value == null and property.enum_values.len == 0) return;
    return error.InvalidBatchRequest;
}

fn isIntegralJsonNumber(value: std.json.Value, numeric_value: f64) bool {
    return switch (value) {
        .integer => true,
        .number_string => |text| blk: {
            _ = std.fmt.parseInt(i64, text, 10) catch break :blk false;
            break :blk true;
        },
        .float => std.math.floor(numeric_value) == numeric_value,
        else => false,
    };
}

fn validateTtlFieldValue(value: std.json.Value) !void {
    if (value == .null) return;
    _ = try parseTtlTimestampNs(value);
}

fn validateRequiredFieldsPresent(object: std.json.ObjectMap, required_fields: []const []const u8) !void {
    for (required_fields) |field_name| {
        if (!object.contains(field_name)) return error.InvalidBatchRequest;
    }
}

fn validateObjectCardinality(object: std.json.ObjectMap, min_properties: ?u64, max_properties: ?u64) !void {
    if (min_properties == null and max_properties == null) return;

    var count: u64 = 0;
    var it = object.iterator();
    while (it.next()) |entry| {
        if (shouldIgnoreSchemaValidationField(entry.key_ptr.*)) continue;
        count += 1;
    }

    if (min_properties) |min_value| {
        if (count < min_value) return error.InvalidBatchRequest;
    }
    if (max_properties) |max_value| {
        if (count > max_value) return error.InvalidBatchRequest;
    }
}

fn makeRootDocumentProperty(document_schema: DocumentSchema) DocumentProperty {
    return .{
        .name = "",
        .field_type = "object",
        .min_properties = document_schema.min_properties,
        .max_properties = document_schema.max_properties,
        .required_fields = document_schema.required_fields,
        .properties = document_schema.properties,
        .pattern_properties = document_schema.pattern_properties,
        .additional_properties_allowed = document_schema.additional_properties_allowed,
        .additional_properties_schema = document_schema.additional_properties_schema,
        .dynamic_infer_types = document_schema.dynamic_infer_types,
        .unevaluated_properties_allowed = document_schema.unevaluated_properties_allowed,
        .unevaluated_properties_schema = document_schema.unevaluated_properties_schema,
        .property_names = document_schema.property_names,
        .dependent_required = document_schema.dependent_required,
        .dependent_schemas = document_schema.dependent_schemas,
        .any_of = document_schema.any_of,
        .one_of = document_schema.one_of,
        .all_of = document_schema.all_of,
        .not_schema = document_schema.not_schema,
        .if_schema = document_schema.if_schema,
        .then_schema = document_schema.then_schema,
        .else_schema = document_schema.else_schema,
    };
}

fn validatePropertyNames(
    context: *RuntimeValidationContext,
    object: std.json.ObjectMap,
    property_names: DocumentProperty,
    enforce_types: bool,
) anyerror!void {
    var it = object.iterator();
    while (it.next()) |entry| {
        if (shouldIgnoreSchemaValidationField(entry.key_ptr.*)) continue;
        const key_value: std.json.Value = .{ .string = entry.key_ptr.* };
        try validateDocumentFieldValueWithContext(context, property_names, &key_value, enforce_types);
    }
}

fn validatePatternProperties(
    context: *RuntimeValidationContext,
    field_name: []const u8,
    value: *const std.json.Value,
    pattern_properties: []const PatternProperty,
    enforce_types: bool,
) anyerror!bool {
    var matched = false;
    for (pattern_properties) |pattern_property| {
        if (!try regexMatch(pattern_property.pattern, field_name)) continue;
        try validateDocumentFieldValueWithContext(context, pattern_property.property.*, value, enforce_types);
        matched = true;
    }
    return matched;
}

fn validateDependentRequired(object: std.json.ObjectMap, dependent_required: []const DependentRequired) !void {
    for (dependent_required) |dependency| {
        if (!object.contains(dependency.name)) continue;
        try validateRequiredFieldsPresent(object, dependency.required_fields);
    }
}

fn validateDependentSchemas(
    context: *RuntimeValidationContext,
    object: std.json.ObjectMap,
    dependent_schemas: []const DependentSchema,
    enforce_types: bool,
) anyerror!void {
    for (dependent_schemas) |dependency| {
        if (!object.contains(dependency.name)) continue;
        const object_value: std.json.Value = .{ .object = object };
        try validateDocumentFieldValueWithContext(context, dependency.schema.*, &object_value, enforce_types);
    }
}

fn schemaMatchesValue(
    context: *RuntimeValidationContext,
    property: DocumentProperty,
    value: *const std.json.Value,
    enforce_types: bool,
) bool {
    _ = enforce_types;
    validateDocumentFieldValueWithContext(context, property, value, false) catch return false;
    return true;
}

fn markAllObjectFieldsEvaluated(
    alloc: std.mem.Allocator,
    object: std.json.ObjectMap,
    evaluated_fields: *std.StringHashMapUnmanaged(void),
) anyerror!void {
    var it = object.iterator();
    while (it.next()) |entry| {
        if (shouldIgnoreSchemaValidationField(entry.key_ptr.*)) continue;
        try evaluated_fields.put(alloc, entry.key_ptr.*, {});
    }
}

fn markDirectObjectFieldCoverage(
    context: *RuntimeValidationContext,
    property: DocumentProperty,
    object: std.json.ObjectMap,
    enforce_types: bool,
    evaluated_fields: *std.StringHashMapUnmanaged(void),
) anyerror!void {
    if (property.root_ref) {
        const root_property = context.root_property orelse return error.InvalidBatchRequest;
        const object_value: std.json.Value = .{ .object = object };
        if (try context.rootRefGuard(&object_value)) |*guard| {
            defer guard.release();
            try markDirectObjectFieldCoverage(context, root_property.*, object, enforce_types, evaluated_fields);
        }
        return;
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        if (shouldIgnoreSchemaValidationField(entry.key_ptr.*)) continue;
        if (findDocumentProperty(property.properties, entry.key_ptr.*) != null) {
            try evaluated_fields.put(context.alloc, entry.key_ptr.*, {});
            continue;
        }
        if (try validatePatternProperties(context, entry.key_ptr.*, entry.value_ptr, property.pattern_properties, enforce_types)) {
            try evaluated_fields.put(context.alloc, entry.key_ptr.*, {});
            continue;
        }
    }

    if (property.additional_properties_schema != null or (property.additional_properties_allowed orelse false)) {
        var remaining = std.StringHashMapUnmanaged(void).empty;
        defer remaining.deinit(context.alloc);
        var object_it = object.iterator();
        while (object_it.next()) |entry| {
            if (shouldIgnoreSchemaValidationField(entry.key_ptr.*)) continue;
            if (evaluated_fields.contains(entry.key_ptr.*)) continue;
            if (property.additional_properties_schema) |schema| {
                try validateDocumentFieldValueWithContext(context, schema.*, entry.value_ptr, enforce_types);
            }
            try remaining.put(context.alloc, entry.key_ptr.*, {});
        }
        var remaining_it = remaining.iterator();
        while (remaining_it.next()) |entry| {
            try evaluated_fields.put(context.alloc, entry.key_ptr.*, {});
        }
    }
}

fn collectComposedObjectFieldCoverage(
    context: *RuntimeValidationContext,
    property: DocumentProperty,
    object: std.json.ObjectMap,
    enforce_types: bool,
    evaluated_fields: *std.StringHashMapUnmanaged(void),
    include_local_unevaluated: bool,
) anyerror!void {
    if (property.root_ref) {
        const root_property = context.root_property orelse return error.InvalidBatchRequest;
        const object_value: std.json.Value = .{ .object = object };
        if (try context.rootRefGuard(&object_value)) |*guard| {
            defer guard.release();
            try collectComposedObjectFieldCoverage(context, root_property.*, object, enforce_types, evaluated_fields, include_local_unevaluated);
        }
        return;
    }

    try markDirectObjectFieldCoverage(context, property, object, enforce_types, evaluated_fields);

    const object_value: std.json.Value = .{ .object = object };
    for (property.all_of) |variant| {
        if (!schemaMatchesValue(context, variant, &object_value, enforce_types)) continue;
        try collectComposedObjectFieldCoverage(context, variant, object, enforce_types, evaluated_fields, true);
    }
    for (property.any_of) |variant| {
        if (!schemaMatchesValue(context, variant, &object_value, enforce_types)) continue;
        try collectComposedObjectFieldCoverage(context, variant, object, enforce_types, evaluated_fields, true);
    }
    for (property.one_of) |variant| {
        if (!schemaMatchesValue(context, variant, &object_value, enforce_types)) continue;
        try collectComposedObjectFieldCoverage(context, variant, object, enforce_types, evaluated_fields, true);
    }
    if (property.if_schema) |if_schema| {
        if (schemaMatchesValue(context, if_schema.*, &object_value, enforce_types)) {
            try collectComposedObjectFieldCoverage(context, if_schema.*, object, enforce_types, evaluated_fields, true);
            if (property.then_schema) |then_schema| {
                if (schemaMatchesValue(context, then_schema.*, &object_value, enforce_types)) {
                    try collectComposedObjectFieldCoverage(context, then_schema.*, object, enforce_types, evaluated_fields, true);
                }
            }
        } else if (property.else_schema) |else_schema| {
            if (schemaMatchesValue(context, else_schema.*, &object_value, enforce_types)) {
                try collectComposedObjectFieldCoverage(context, else_schema.*, object, enforce_types, evaluated_fields, true);
            }
        }
    }
    for (property.dependent_schemas) |dependency| {
        if (!object.contains(dependency.name)) continue;
        if (!schemaMatchesValue(context, dependency.schema.*, &object_value, enforce_types)) continue;
        try collectComposedObjectFieldCoverage(context, dependency.schema.*, object, enforce_types, evaluated_fields, true);
    }

    if (!include_local_unevaluated) return;

    if (property.unevaluated_properties_schema != null) {
        try markAllObjectFieldsEvaluated(context.alloc, object, evaluated_fields);
        return;
    }
    if (property.unevaluated_properties_allowed) |allowed| {
        if (allowed) try markAllObjectFieldsEvaluated(context.alloc, object, evaluated_fields);
    }
}

fn markDirectArrayIndicesEvaluated(
    alloc: std.mem.Allocator,
    property: DocumentProperty,
    array: std.json.Array,
    evaluated_indices: *std.AutoHashMapUnmanaged(usize, void),
) anyerror!void {
    const prefix_len = @min(property.prefix_items.len, array.items.len);
    for (0..prefix_len) |index| try evaluated_indices.put(alloc, index, {});
    if (property.item != null) {
        for (prefix_len..array.items.len) |index| try evaluated_indices.put(alloc, index, {});
    }
}

fn collectEvaluatedArrayIndices(
    context: *RuntimeValidationContext,
    property: DocumentProperty,
    value: *const std.json.Value,
    enforce_types: bool,
    evaluated_indices: *std.AutoHashMapUnmanaged(usize, void),
    include_local_unevaluated: bool,
) anyerror!void {
    if (property.root_ref) {
        const root_property = context.root_property orelse return error.InvalidBatchRequest;
        if (try context.rootRefGuard(value)) |*guard| {
            defer guard.release();
            try collectEvaluatedArrayIndices(context, root_property.*, value, enforce_types, evaluated_indices, include_local_unevaluated);
        }
        return;
    }

    const array = switch (value.*) {
        .array => |array| array,
        else => return error.InvalidBatchRequest,
    };

    try markDirectArrayIndicesEvaluated(context.alloc, property, array, evaluated_indices);

    if (property.contains_schema) |contains_schema| {
        for (array.items, 0..) |item_value, index| {
            if (!schemaMatchesValue(context, contains_schema.*, &item_value, enforce_types)) continue;
            try evaluated_indices.put(context.alloc, index, {});
        }
    }

    for (property.all_of) |variant| {
        if (!schemaMatchesValue(context, variant, value, enforce_types)) continue;
        try collectEvaluatedArrayIndices(context, variant, value, enforce_types, evaluated_indices, true);
    }
    for (property.any_of) |variant| {
        if (!schemaMatchesValue(context, variant, value, enforce_types)) continue;
        try collectEvaluatedArrayIndices(context, variant, value, enforce_types, evaluated_indices, true);
    }
    for (property.one_of) |variant| {
        if (!schemaMatchesValue(context, variant, value, enforce_types)) continue;
        try collectEvaluatedArrayIndices(context, variant, value, enforce_types, evaluated_indices, true);
    }
    if (property.if_schema) |if_schema| {
        if (schemaMatchesValue(context, if_schema.*, value, enforce_types)) {
            try collectEvaluatedArrayIndices(context, if_schema.*, value, enforce_types, evaluated_indices, true);
            if (property.then_schema) |then_schema| {
                if (schemaMatchesValue(context, then_schema.*, value, enforce_types)) {
                    try collectEvaluatedArrayIndices(context, then_schema.*, value, enforce_types, evaluated_indices, true);
                }
            }
        } else if (property.else_schema) |else_schema| {
            if (schemaMatchesValue(context, else_schema.*, value, enforce_types)) {
                try collectEvaluatedArrayIndices(context, else_schema.*, value, enforce_types, evaluated_indices, true);
            }
        }
    }

    if (!include_local_unevaluated) return;

    if (property.unevaluated_items_schema != null) {
        for (0..array.items.len) |index| try evaluated_indices.put(context.alloc, index, {});
        return;
    }
    if (property.unevaluated_items_allowed) |allowed| {
        if (allowed) {
            for (0..array.items.len) |index| try evaluated_indices.put(context.alloc, index, {});
        }
    }
}

fn jsonValueEqual(left: std.json.Value, right: std.json.Value) bool {
    if (std.meta.activeTag(left) != std.meta.activeTag(right)) return false;

    return switch (left) {
        .null => true,
        .bool => |bool_value| bool_value == right.bool,
        .integer => |integer| integer == right.integer,
        .float => |float_value| float_value == right.float,
        .number_string => |number_string| std.mem.eql(u8, number_string, right.number_string),
        .string => |string_value| std.mem.eql(u8, string_value, right.string),
        .array => |array| blk: {
            if (array.items.len != right.array.items.len) break :blk false;
            for (array.items, right.array.items) |left_item, right_item| {
                if (!jsonValueEqual(left_item, right_item)) break :blk false;
            }
            break :blk true;
        },
        .object => |object| blk: {
            if (object.count() != right.object.count()) break :blk false;
            var it = object.iterator();
            while (it.next()) |entry| {
                const right_value = right.object.get(entry.key_ptr.*) orelse break :blk false;
                if (!jsonValueEqual(entry.value_ptr.*, right_value)) break :blk false;
            }
            break :blk true;
        },
    };
}

fn parseJsonNumber(value: std.json.Value) !f64 {
    return switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| float,
        .number_string => |num| std.fmt.parseFloat(f64, num),
        else => error.InvalidNumber,
    };
}

fn isMultipleOf(value: f64, divisor: f64) bool {
    const quotient = value / divisor;
    return std.math.approxEqAbs(f64, quotient, @round(quotient), 1e-9);
}

fn regexMatch(pattern: []const u8, text: []const u8) !bool {
    return schema_regex.matches(std.heap.page_allocator, pattern, text) catch |err| switch (err) {
        error.InvalidRegex => error.InvalidSchemaUpdateRequest,
        else => err,
    };
}

fn parseTtlTimestampNs(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |integer| blk: {
            if (integer < 0) return error.InvalidBatchRequest;
            break :blk std.math.cast(u64, integer) orelse return error.InvalidBatchRequest;
        },
        .number_string => |text| std.fmt.parseInt(u64, std.mem.trim(u8, text, " \t\r\n"), 10) catch return error.InvalidBatchRequest,
        .string => |text| try parseTtlStringTimestampNs(text),
        else => error.InvalidBatchRequest,
    };
}

fn parseTtlStringTimestampNs(text: []const u8) !u64 {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len == 0) return error.InvalidBatchRequest;
    if (std.fmt.parseInt(u64, trimmed, 10)) |ts| return ts else |_| {}
    return parseRfc3339ToNs(trimmed) orelse error.InvalidBatchRequest;
}

fn parseRfc3339ToNs(text: []const u8) ?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;

    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;

    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;

    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) @as(i64, 1) else @as(i64, 0);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

fn validateStringFormat(format: []const u8, string_value: []const u8) !void {
    if (std.mem.eql(u8, format, "email")) {
        if (!isValidEmail(string_value)) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "date-time")) {
        if (parseRfc3339ToNs(string_value) == null) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "date")) {
        if (!isValidDate(string_value)) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "uuid")) {
        if (!isValidUuid(string_value)) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "ipv4")) {
        _ = std.Io.net.Ip4Address.parse(string_value, 0) catch return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "ipv6")) {
        _ = std.Io.net.Ip6Address.parse(string_value, 0) catch return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "hostname")) {
        if (!isValidHostname(string_value)) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "uri")) {
        _ = std.Uri.parse(string_value) catch return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "uri-reference")) {
        if (!isValidUriReference(string_value)) return error.InvalidBatchRequest;
        return;
    }
}

fn isValidEmail(value: []const u8) bool {
    if (value.len < 3 or std.mem.indexOfScalar(u8, value, ' ') != null) return false;
    const at_index = std.mem.indexOfScalar(u8, value, '@') orelse return false;
    if (at_index == 0 or at_index == value.len - 1) return false;
    if (std.mem.lastIndexOfScalar(u8, value, '@') != at_index) return false;
    const domain = value[at_index + 1 ..];
    const dot_index = std.mem.indexOfScalar(u8, domain, '.') orelse return false;
    if (dot_index == 0 or dot_index == domain.len - 1) return false;
    return true;
}

fn isValidDate(value: []const u8) bool {
    if (value.len != 10 or value[4] != '-' or value[7] != '-') return false;
    const year = std.fmt.parseInt(i64, value[0..4], 10) catch return false;
    const month = std.fmt.parseInt(i64, value[5..7], 10) catch return false;
    const day = std.fmt.parseInt(i64, value[8..10], 10) catch return false;
    return civilDateTimeToNs(year, month, day, 0, 0, 0, 0) != null;
}

fn isValidHostname(value: []const u8) bool {
    if (value.len == 0 or value.len > 253) return false;
    var labels = std.mem.splitScalar(u8, value, '.');
    while (labels.next()) |label| {
        if (label.len == 0 or label.len > 63) return false;
        if (label[0] == '-' or label[label.len - 1] == '-') return false;
        for (label) |ch| {
            if (std.ascii.isAlphanumeric(ch) or ch == '-') continue;
            return false;
        }
    }
    return true;
}

fn isValidUriReference(value: []const u8) bool {
    if (value.len == 0) return true;
    if (std.Uri.parse(value)) |_| return true else |_| {}
    for (value) |ch| {
        if (std.ascii.isWhitespace(ch) or std.ascii.isControl(ch)) return false;
    }
    return true;
}

fn civilDateTimeToNs(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64, nanos: u64) ?u64 {
    if (month < 1 or month > 12) return null;
    if (day < 1 or day > 31) return null;
    if (hour < 0 or hour > 23) return null;
    if (minute < 0 or minute > 59) return null;
    if (second < 0 or second > 60) return null;
    if (nanos >= std.time.ns_per_s) return null;

    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn isValidUuid(value: []const u8) bool {
    if (value.len != 36) return false;
    for (value, 0..) |ch, i| {
        switch (i) {
            8, 13, 18, 23 => if (ch != '-') return false,
            else => if (!std.ascii.isHex(ch)) return false,
        }
    }
    return true;
}

test "parse schema and validate document writes" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"dynamic_templates\":{\"meta\":{\"match\":\"meta_*\",\"mapping\":{\"type\":\"keyword\"}}},\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"published\":{\"type\":\"boolean\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"published\":true,\"meta_status\":\"draft\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }}),
    );
}

test "relational embedded document schema is scoped to explicit json columns" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"attrs\":{\"type\":\"json\",\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}},\"additionalProperties\":true},\"dynamic_templates\":{\"metrics\":{\"path_match\":\"metrics.*\",\"mapping\":{\"type\":\"numeric\"}}}}},\"required\":[\"id\"],\"additionalProperties\":false}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try std.testing.expectEqual(StorageMode.relational, parsed.storage_mode);
    try std.testing.expectEqual(@as(usize, 1), parsed.document_schemas[0].properties[1].embedded_dynamic_templates.len);

    var derived_index_shape = try parseSchema(
        std.testing.allocator,
        "{\"version\":4,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"body\":{\"type\":\"text\"},\"embedding\":{\"type\":\"embedding\"},\"source_doc\":{\"type\":\"keyword\"},\"target_doc\":{\"type\":\"keyword\"},\"edge_type\":{\"type\":\"keyword\"},\"confidence\":{\"type\":\"numeric\"},\"attrs\":{\"type\":\"json\",\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"plan\":{\"type\":\"keyword\"},\"source\":{\"type\":\"keyword\"},\"target\":{\"type\":\"keyword\"},\"edge_type\":{\"type\":\"keyword\"},\"confidence\":{\"type\":\"numeric\"}},\"additionalProperties\":true}}},\"required\":[\"id\"],\"additionalProperties\":false}}},\"primary_key\":{\"columns\":[\"id\"]}}",
    );
    defer derived_index_shape.deinit(std.testing.allocator);
    try std.testing.expectEqual(StorageMode.relational, derived_index_shape.storage_mode);
    try std.testing.expectEqual(@as(usize, 8), derived_index_shape.document_schemas[0].properties.len);

    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\",\"schema\":{\"type\":\"object\"}}},\"required\":[\"id\"],\"additionalProperties\":false}}}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"attrs\":{\"type\":\"object\",\"dynamic_templates\":{\"metrics\":{\"path_match\":\"metrics.*\",\"mapping\":{\"type\":\"numeric\"}}}}},\"required\":[\"id\"],\"additionalProperties\":false}}}}",
        ),
    );
}

test "relational schema parses primary-key foreign keys and unique constraints" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"customer_id\":{\"type\":\"keyword\"},\"email\":{\"type\":\"keyword\"}},\"required\":[\"id\",\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_delete\":\"restrict\",\"on_update\":\"no_action\",\"timing\":\"immediate\",\"match\":\"simple\",\"validation_state\":\"enforced\"},{\"name\":\"orders_customer_email_fkey\",\"columns\":[\"customer_id\",\"email\"],\"references\":{\"table\":\"order\",\"columns\":[\"customer_id\",\"email\"]},\"on_delete\":\"no_action\",\"on_update\":\"restrict\"}],\"unique_constraints\":[{\"name\":\"orders_customer_email_key\",\"columns\":[\"customer_id\",\"email\"]}]}",
    );
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), parsed.foreign_keys.len);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", parsed.foreign_keys[0].name);
    try std.testing.expectEqualStrings("customer_id", parsed.foreign_keys[0].columns[0]);
    try std.testing.expectEqualStrings("customers", parsed.foreign_keys[0].references.table);
    try std.testing.expectEqualStrings("_id", parsed.foreign_keys[0].references.columns[0]);
    try std.testing.expectEqual(ForeignKeyAction.no_action, parsed.foreign_keys[0].on_update);
    try std.testing.expectEqual(ForeignKeyTiming.immediate, parsed.foreign_keys[0].timing);
    try std.testing.expectEqual(ForeignKeyMatch.simple, parsed.foreign_keys[0].match);
    try std.testing.expectEqual(ForeignKeyValidationState.enforced, parsed.foreign_keys[0].validation_state);
    try std.testing.expectEqualStrings("orders_customer_email_fkey", parsed.foreign_keys[1].name);
    try std.testing.expectEqual(ForeignKeyAction.no_action, parsed.foreign_keys[1].on_delete);
    try std.testing.expectEqual(@as(usize, 2), parsed.foreign_keys[1].columns.len);
    try std.testing.expectEqualStrings("customer_id", parsed.foreign_keys[1].columns[0]);
    try std.testing.expectEqualStrings("email", parsed.foreign_keys[1].columns[1]);
    try std.testing.expectEqual(@as(usize, 2), parsed.foreign_keys[1].references.columns.len);
    try std.testing.expectEqualStrings("customer_id", parsed.foreign_keys[1].references.columns[0]);
    try std.testing.expectEqualStrings("email", parsed.foreign_keys[1].references.columns[1]);
    try std.testing.expectEqual(ForeignKeyAction.restrict, parsed.foreign_keys[1].on_update);
    try std.testing.expectEqual(@as(usize, 1), parsed.unique_constraints.len);
    try std.testing.expectEqualStrings("orders_customer_email_key", parsed.unique_constraints[0].name);
    try std.testing.expectEqual(@as(usize, 2), parsed.unique_constraints[0].columns.len);
    try std.testing.expectEqualStrings("customer_id", parsed.unique_constraints[0].columns[0]);
    try std.testing.expectEqualStrings("email", parsed.unique_constraints[0].columns[1]);

    var parsed_set_null = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_delete\":\"set_null\"}]}",
    );
    defer parsed_set_null.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyAction.set_null, parsed_set_null.foreign_keys[0].on_delete);
    var parsed_update_set_null = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_update\":\"set_null\"}]}",
    );
    defer parsed_update_set_null.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyAction.set_null, parsed_update_set_null.foreign_keys[0].on_update);

    var parsed_cascade = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"id\",\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_delete\":\"cascade\"}]}",
    );
    defer parsed_cascade.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyAction.cascade, parsed_cascade.foreign_keys[0].on_delete);
    var parsed_update_cascade = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"id\",\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_update\":\"cascade\"}]}",
    );
    defer parsed_update_cascade.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyAction.cascade, parsed_update_cascade.foreign_keys[0].on_update);
}

test "relational schema parses application-time temporal constraints" {
    var parsed = try parseSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"price","enforce_types":true,"document_schemas":{"price":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"sku":{"type":"keyword"},"valid_from":{"type":"datetime"},"valid_to":{"type":"datetime"}},"required":["tenant_id","sku","valid_from","valid_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to","range_type":"daterange"}],"primary_key":{"columns":["tenant_id","sku"],"without_overlaps_period":"valid_time","deferrable":true,"timing":"deferred"},"unique_constraints":[{"name":"price_sku_time_key","columns":["sku"],"without_overlaps_period":"valid_time","deferrable":"DEFERRABLE INITIALLY DEFERRED"}],"foreign_keys":[{"name":"price_parent_time_fkey","columns":["tenant_id","sku"],"period":"valid_time","references":{"table":"parent_prices","columns":["tenant_id","sku"],"period":"valid_time"}}]}
    );
    defer parsed.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), parsed.periods.len);
    try std.testing.expectEqualStrings("valid_time", parsed.periods[0].name);
    try std.testing.expectEqualStrings("valid_from", parsed.periods[0].start_column);
    try std.testing.expectEqualStrings("valid_to", parsed.periods[0].end_column);
    try std.testing.expectEqual(storage_schema.RelationalPeriodRangeType.daterange, parsed.periods[0].range_type.?);
    try std.testing.expect(parsed.primary_key != null);
    try std.testing.expectEqualStrings("valid_time", parsed.primary_key.?.without_overlaps_period.?);
    try std.testing.expect(parsed.primary_key.?.deferrable);
    try std.testing.expectEqual(ForeignKeyTiming.deferred, parsed.primary_key.?.timing);
    try std.testing.expectEqualStrings("valid_time", parsed.unique_constraints[0].without_overlaps_period.?);
    try std.testing.expect(parsed.unique_constraints[0].deferrable);
    try std.testing.expectEqual(ForeignKeyTiming.deferred, parsed.unique_constraints[0].timing);
    try std.testing.expectEqualStrings("valid_time", parsed.foreign_keys[0].period.?);
    try std.testing.expectEqualStrings("valid_time", parsed.foreign_keys[0].references.period.?);

    var parsed_delete_cascade = try parseSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"price","enforce_types":true,"document_schemas":{"price":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"sku":{"type":"keyword"},"valid_from":{"type":"datetime"},"valid_to":{"type":"datetime"}},"required":["tenant_id","sku","valid_from","valid_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["tenant_id","sku"],"without_overlaps_period":"valid_time"},"foreign_keys":[{"name":"price_parent_time_fkey","columns":["tenant_id","sku"],"period":"valid_time","references":{"table":"parent_prices","columns":["tenant_id","sku"],"period":"valid_time"},"on_delete":"cascade"}]}
    );
    defer parsed_delete_cascade.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyAction.cascade, parsed_delete_cascade.foreign_keys[0].on_delete);

    var parsed_delete_set_null = try parseSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"price","enforce_types":true,"document_schemas":{"price":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"sku":{"type":"keyword"},"adjustment_id":{"type":"keyword"},"valid_from":{"type":"datetime"},"valid_to":{"type":"datetime"}},"required":["adjustment_id","valid_from","valid_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["adjustment_id"],"without_overlaps_period":"valid_time"},"foreign_keys":[{"name":"price_parent_time_fkey","columns":["tenant_id","sku"],"period":"valid_time","references":{"table":"parent_prices","columns":["tenant_id","sku"],"period":"valid_time"},"on_delete":"set_null"}]}
    );
    defer parsed_delete_set_null.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyAction.set_null, parsed_delete_set_null.foreign_keys[0].on_delete);

    var parsed_update_set_null = try parseSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"price","enforce_types":true,"document_schemas":{"price":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"sku":{"type":"keyword"},"adjustment_id":{"type":"keyword"},"valid_from":{"type":"datetime"},"valid_to":{"type":"datetime"}},"required":["adjustment_id","valid_from","valid_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["adjustment_id"],"without_overlaps_period":"valid_time"},"foreign_keys":[{"name":"price_parent_time_fkey","columns":["tenant_id","sku"],"period":"valid_time","references":{"table":"parent_prices","columns":["tenant_id","sku"],"period":"valid_time"},"on_update":"set_null"}]}
    );
    defer parsed_update_set_null.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyAction.set_null, parsed_update_set_null.foreign_keys[0].on_update);

    var parsed_update_cascade = try parseSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"price","enforce_types":true,"document_schemas":{"price":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"sku":{"type":"keyword"},"valid_from":{"type":"datetime"},"valid_to":{"type":"datetime"}},"required":["tenant_id","sku","valid_from","valid_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["tenant_id","sku"],"without_overlaps_period":"valid_time"},"foreign_keys":[{"name":"price_parent_time_fkey","columns":["tenant_id","sku"],"period":"valid_time","references":{"table":"parent_prices","columns":["tenant_id","sku"],"period":"valid_time"},"on_update":"cascade"}]}
    );
    defer parsed_update_cascade.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyAction.cascade, parsed_update_cascade.foreign_keys[0].on_update);

    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(std.testing.allocator,
            \\{"storage_mode":"relational","default_type":"price","enforce_types":true,"document_schemas":{"price":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"valid_from":{"type":"datetime"},"valid_to":{"type":"datetime"}},"required":["tenant_id","valid_from"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["tenant_id"],"without_overlaps_period":"missing_time"}}
        ),
    );
}

test "relational schema parses and validates composite primary keys" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant_id\":{\"type\":\"keyword\"},\"order_id\":{\"type\":\"keyword\"},\"customer_id\":{\"type\":\"keyword\"},\"status\":{\"type\":\"keyword\"}},\"required\":[\"tenant_id\",\"order_id\"],\"additionalProperties\":false}}},\"primary_key\":{\"name\":\"orders_pkey\",\"columns\":[\"tenant_id\",\"order_id\"],\"include_columns\":[\"status\",\"customer_id\"]},\"foreign_keys\":[{\"name\":\"order_parent_fkey\",\"columns\":[\"tenant_id\",\"customer_id\"],\"references\":{\"table\":\"order\",\"columns\":[\"tenant_id\",\"order_id\"]},\"on_delete\":\"restrict\"}]}",
    );
    defer parsed.deinit(std.testing.allocator);

    try std.testing.expect(parsed.primary_key != null);
    try std.testing.expectEqualStrings("orders_pkey", parsed.primary_key.?.name.?);
    try std.testing.expectEqual(@as(usize, 2), parsed.primary_key.?.columns.len);
    try std.testing.expectEqualStrings("tenant_id", parsed.primary_key.?.columns[0]);
    try std.testing.expectEqualStrings("order_id", parsed.primary_key.?.columns[1]);
    try std.testing.expectEqual(@as(usize, 2), parsed.primary_key.?.include_columns.len);
    try std.testing.expectEqualStrings("status", parsed.primary_key.?.include_columns[0]);
    try std.testing.expectEqualStrings("customer_id", parsed.primary_key.?.include_columns[1]);
    try std.testing.expectEqual(@as(usize, 1), parsed.foreign_keys.len);
    try std.testing.expectEqualStrings("tenant_id", parsed.foreign_keys[0].references.columns[0]);
    try std.testing.expectEqualStrings("order_id", parsed.foreign_keys[0].references.columns[1]);

    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant_id\":{\"type\":\"keyword\"},\"order_id\":{\"type\":\"keyword\"}},\"required\":[\"tenant_id\"],\"additionalProperties\":false}}},\"primary_key\":{\"columns\":[\"tenant_id\",\"order_id\"]}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant_id\":{\"type\":\"keyword\"}},\"required\":[\"tenant_id\"],\"additionalProperties\":false}}},\"primary_key\":{\"columns\":[\"_id\",\"tenant_id\"]}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant_id\":{\"type\":\"keyword\"},\"payload\":{\"type\":\"json\"}},\"required\":[\"tenant_id\",\"payload\"],\"additionalProperties\":false}}},\"primary_key\":{\"columns\":[\"tenant_id\",\"payload\"]}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant_id\":{\"type\":\"keyword\"},\"order_id\":{\"type\":\"keyword\"},\"email\":{\"type\":\"keyword\"}},\"required\":[\"tenant_id\",\"order_id\"],\"additionalProperties\":false}}},\"primary_key\":{\"name\":\"orders_email_key\",\"columns\":[\"tenant_id\",\"order_id\"]},\"unique_constraints\":[{\"name\":\"orders_email_key\",\"columns\":[\"email\"]}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant_id\":{\"type\":\"keyword\"},\"order_id\":{\"type\":\"keyword\"}},\"required\":[\"tenant_id\",\"order_id\"],\"additionalProperties\":false}}},\"primary_key\":{\"columns\":[\"tenant_id\",\"order_id\"],\"include_columns\":[\"tenant_id\"]}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant_id\":{\"type\":\"keyword\"},\"order_id\":{\"type\":\"keyword\"},\"status\":{\"type\":\"keyword\"}},\"required\":[\"tenant_id\",\"order_id\"],\"additionalProperties\":false}}},\"primary_key\":{\"columns\":[\"tenant_id\",\"order_id\"],\"include_columns\":[\"status\",\"status\"]}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant_id\":{\"type\":\"keyword\"},\"order_id\":{\"type\":\"keyword\"}},\"required\":[\"tenant_id\",\"order_id\"],\"additionalProperties\":false}}},\"primary_key\":{\"columns\":[\"tenant_id\",\"order_id\"],\"include_columns\":[\"missing\"]}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tenant_id\":{\"type\":\"keyword\"},\"order_id\":{\"type\":\"keyword\"},\"payload\":{\"type\":\"json\"}},\"required\":[\"tenant_id\",\"order_id\"],\"additionalProperties\":false}}},\"primary_key\":{\"columns\":[\"tenant_id\",\"order_id\"],\"include_columns\":[\"payload\"]}}",
        ),
    );
}

test "relational schema accepts SQL foreign key enum spellings" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_delete\":\"SET NULL\",\"on_update\":\"NO ACTION\",\"timing\":\"INITIALLY DEFERRED\",\"match\":\"MATCH SIMPLE\",\"validation_state\":\"UNVALIDATED\"}]}",
    );
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyAction.set_null, parsed.foreign_keys[0].on_delete);
    try std.testing.expectEqual(ForeignKeyAction.no_action, parsed.foreign_keys[0].on_update);
    try std.testing.expectEqual(ForeignKeyTiming.deferred, parsed.foreign_keys[0].timing);
    try std.testing.expect(parsed.foreign_keys[0].deferrable);
    try std.testing.expectEqual(ForeignKeyMatch.simple, parsed.foreign_keys[0].match);
    try std.testing.expectEqual(ForeignKeyValidationState.unvalidated, parsed.foreign_keys[0].validation_state);

    var parsed_sql_not_valid = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"validation_state\":\"NOT VALID\"}]}",
    );
    defer parsed_sql_not_valid.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyValidationState.unvalidated, parsed_sql_not_valid.foreign_keys[0].validation_state);

    var parsed_hyphenated = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_delete\":\"no-action\",\"on_update\":\"RESTRICT\",\"timing\":\"initially-immediate\",\"deferrable\":true,\"match\":\"match-full\",\"validation_state\":\"not-valid\"}]}",
    );
    defer parsed_hyphenated.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyAction.no_action, parsed_hyphenated.foreign_keys[0].on_delete);
    try std.testing.expectEqual(ForeignKeyAction.restrict, parsed_hyphenated.foreign_keys[0].on_update);
    try std.testing.expectEqual(ForeignKeyTiming.immediate, parsed_hyphenated.foreign_keys[0].timing);
    try std.testing.expect(parsed_hyphenated.foreign_keys[0].deferrable);
    try std.testing.expectEqual(ForeignKeyMatch.full, parsed_hyphenated.foreign_keys[0].match);
    try std.testing.expectEqual(ForeignKeyValidationState.unvalidated, parsed_hyphenated.foreign_keys[0].validation_state);

    var parsed_clause_actions = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_delete\":\"ON DELETE CASCADE\",\"on_update\":\"ON UPDATE SET NULL\"}]}",
    );
    defer parsed_clause_actions.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyAction.cascade, parsed_clause_actions.foreign_keys[0].on_delete);
    try std.testing.expectEqual(ForeignKeyAction.set_null, parsed_clause_actions.foreign_keys[0].on_update);

    var parsed_non_mutating_clause_actions = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_delete\":\"ON DELETE NO ACTION\",\"on_update\":\"ON UPDATE RESTRICT\"}]}",
    );
    defer parsed_non_mutating_clause_actions.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyAction.no_action, parsed_non_mutating_clause_actions.foreign_keys[0].on_delete);
    try std.testing.expectEqual(ForeignKeyAction.restrict, parsed_non_mutating_clause_actions.foreign_keys[0].on_update);

    var parsed_whitespace_clause = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_delete\":\"ON\\tDELETE\\tCASCADE\",\"on_update\":\"ON\\nUPDATE\\nSET\\nNULL\",\"timing\":\"DEFERRABLE\\tINITIALLY\\tDEFERRED\",\"match\":\"MATCH\\nFULL\",\"validation_state\":\"NOT\\tVALID\"}]}",
    );
    defer parsed_whitespace_clause.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyAction.cascade, parsed_whitespace_clause.foreign_keys[0].on_delete);
    try std.testing.expectEqual(ForeignKeyAction.set_null, parsed_whitespace_clause.foreign_keys[0].on_update);
    try std.testing.expectEqual(ForeignKeyTiming.deferred, parsed_whitespace_clause.foreign_keys[0].timing);
    try std.testing.expect(parsed_whitespace_clause.foreign_keys[0].deferrable);
    try std.testing.expectEqual(ForeignKeyMatch.full, parsed_whitespace_clause.foreign_keys[0].match);
    try std.testing.expectEqual(ForeignKeyValidationState.unvalidated, parsed_whitespace_clause.foreign_keys[0].validation_state);

    var parsed_sql_deferrable = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"timing\":\"INITIALLY IMMEDIATE\",\"deferrable\":\"DEFERRABLE\"}]}",
    );
    defer parsed_sql_deferrable.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyTiming.immediate, parsed_sql_deferrable.foreign_keys[0].timing);
    try std.testing.expect(parsed_sql_deferrable.foreign_keys[0].deferrable);

    var parsed_sql_not_deferrable = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"timing\":\"INITIALLY IMMEDIATE\",\"deferrable\":\"NOT DEFERRABLE\"}]}",
    );
    defer parsed_sql_not_deferrable.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyTiming.immediate, parsed_sql_not_deferrable.foreign_keys[0].timing);
    try std.testing.expect(!parsed_sql_not_deferrable.foreign_keys[0].deferrable);

    var parsed_combined_deferrable = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"deferrable\":\"DEFERRABLE INITIALLY DEFERRED\"}]}",
    );
    defer parsed_combined_deferrable.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyTiming.deferred, parsed_combined_deferrable.foreign_keys[0].timing);
    try std.testing.expect(parsed_combined_deferrable.foreign_keys[0].deferrable);

    var parsed_timing_deferrability = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"timing\":\"NOT DEFERRABLE INITIALLY IMMEDIATE\"}]}",
    );
    defer parsed_timing_deferrability.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyTiming.immediate, parsed_timing_deferrability.foreign_keys[0].timing);
    try std.testing.expect(!parsed_timing_deferrability.foreign_keys[0].deferrable);

    var parsed_timing_combined_deferrable = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"timing\":\"DEFERRABLE INITIALLY IMMEDIATE\"}]}",
    );
    defer parsed_timing_combined_deferrable.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyTiming.immediate, parsed_timing_combined_deferrable.foreign_keys[0].timing);
    try std.testing.expect(parsed_timing_combined_deferrable.foreign_keys[0].deferrable);

    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_delete\":\"SET DEFAULT\"}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"timing\":\"deferred\",\"deferrable\":false}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"timing\":\"INITIALLY DEFERRED\",\"deferrable\":\"NOT DEFERRABLE\"}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"timing\":\"INITIALLY IMMEDIATE\",\"deferrable\":\"DEFERRABLE INITIALLY DEFERRED\"}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"deferrable\":\"NOT DEFERRABLE INITIALLY DEFERRED\"}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"timing\":\"NOT DEFERRABLE INITIALLY DEFERRED\"}]}",
        ),
    );
}

test "relational schema rejects unsupported foreign key shapes" {
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"order\",\"columns\":[\"email\"]},\"on_delete\":\"restrict\"}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"},\"email\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}},\"unique_constraints\":[{\"name\":\"orders_email_key\",\"columns\":[\"email\"]}],\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"order\",\"columns\":[\"missing_email\"]},\"on_delete\":\"restrict\"}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"numeric\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_delete\":\"restrict\"}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_delete\":\"set_null\"}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"on_update\":\"set_null\"}]}",
        ),
    );
    var parsed_deferred = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"timing\":\"deferred\"}]}",
    );
    defer parsed_deferred.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyTiming.deferred, parsed_deferred.foreign_keys[0].timing);
    try std.testing.expect(parsed_deferred.foreign_keys[0].deferrable);
    var parsed_match_simple = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"match\":\"simple\"}]}",
    );
    defer parsed_match_simple.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyMatch.simple, parsed_match_simple.foreign_keys[0].match);
    var parsed_match_full = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"match\":\"full\"}]}",
    );
    defer parsed_match_full.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyMatch.full, parsed_match_full.foreign_keys[0].match);
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"match\":\"partial\"}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"],\"schema\":\"public\"}}]}",
        ),
    );
    var parsed_unvalidated = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"validation_state\":\"unvalidated\"}]}",
    );
    defer parsed_unvalidated.deinit(std.testing.allocator);
    try std.testing.expectEqual(ForeignKeyValidationState.unvalidated, parsed_unvalidated.foreign_keys[0].validation_state);
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"validation_state\":\"validating\"}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]},\"validation_state\":\"invalid\"}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\",\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]}}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"},\"customer_email\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"bad\",\"columns\":[\"customer_id\",\"customer_email\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\",\"email\"]}}]}",
        ),
    );
    var parsed_shared_columns = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_customers_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]}},{\"name\":\"orders_customer_id_accounts_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"accounts\",\"columns\":[\"_id\"]}}]}",
    );
    defer parsed_shared_columns.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), parsed_shared_columns.foreign_keys.len);
    try std.testing.expectEqualStrings("orders_customer_id_customers_fkey", parsed_shared_columns.foreign_keys[0].name);
    try std.testing.expectEqualStrings("orders_customer_id_accounts_fkey", parsed_shared_columns.foreign_keys[1].name);
    try std.testing.expectEqualStrings("customer_id", parsed_shared_columns.foreign_keys[0].columns[0]);
    try std.testing.expectEqualStrings("customer_id", parsed_shared_columns.foreign_keys[1].columns[0]);
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_id\":{\"type\":\"keyword\"}},\"required\":[\"customer_id\"],\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"customers\",\"columns\":[\"_id\"]}},{\"name\":\"orders_customer_id_fkey\",\"columns\":[\"customer_id\"],\"references\":{\"table\":\"accounts\",\"columns\":[\"_id\"]}}]}",
        ),
    );
}

test "relational schema admits cross-table unique foreign key targets for hosted resolution" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"order\",\"enforce_types\":true,\"document_schemas\":{\"order\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer_email\":{\"type\":\"keyword\"},\"region\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}},\"foreign_keys\":[{\"name\":\"orders_customer_email_fkey\",\"columns\":[\"customer_email\",\"region\"],\"references\":{\"table\":\"customers\",\"columns\":[\"email\",\"region\"]},\"on_delete\":\"restrict\"}]}",
    );
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), parsed.foreign_keys.len);
    try std.testing.expectEqualStrings("orders_customer_email_fkey", parsed.foreign_keys[0].name);
    try std.testing.expectEqualStrings("customers", parsed.foreign_keys[0].references.table);
    try std.testing.expectEqualStrings("email", parsed.foreign_keys[0].references.columns[0]);
    try std.testing.expectEqualStrings("region", parsed.foreign_keys[0].references.columns[1]);
}

test "relational schema rejects unsupported unique constraint shapes" {
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"keyword\"}},\"required\":[],\"additionalProperties\":false}}},\"unique_constraints\":[{\"name\":\"bad\",\"columns\":[\"email\",\"email\"]}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"keyword\"},\"payload\":{\"type\":\"json\"}},\"required\":[],\"additionalProperties\":false}}},\"unique_constraints\":[{\"name\":\"bad\",\"columns\":[\"email\",\"payload\"]}]}",
        ),
    );
    var parsed_partial = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"keyword\"},\"status\":{\"type\":\"keyword\"},\"display_name\":{\"type\":\"text\"}},\"required\":[],\"additionalProperties\":false}}},\"unique_constraints\":[{\"name\":\"email_present_key\",\"columns\":[\"email\"],\"include_columns\":[\"status\",\"display_name\"],\"nulls_not_distinct\":true,\"where\":{\"all\":[{\"field\":\"email\",\"op\":\"is_not_null\"},{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"}]}},{\"name\":\"email_lower_key\",\"expressions\":[{\"op\":\"lower\",\"field\":\"email\"}],\"validation_state\":\"unvalidated\"},{\"name\":\"email_upper_key\",\"expressions\":[{\"op\":\"upper\",\"field\":\"email\"}]},{\"name\":\"email_md5_key\",\"expressions\":[{\"op\":\"md5\",\"field\":\"email\"}]}]}",
    );
    defer parsed_partial.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 4), parsed_partial.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 2), parsed_partial.unique_constraints[0].include_columns.len);
    try std.testing.expectEqualStrings("status", parsed_partial.unique_constraints[0].include_columns[0]);
    try std.testing.expectEqualStrings("display_name", parsed_partial.unique_constraints[0].include_columns[1]);
    try std.testing.expect(parsed_partial.unique_constraints[0].nulls_not_distinct);
    try std.testing.expectEqual(@as(usize, 2), parsed_partial.unique_constraints[0].where.len);
    try std.testing.expectEqualStrings("email", parsed_partial.unique_constraints[0].where[0].field);
    try std.testing.expectEqual(UniquePredicateOp.is_not_null, parsed_partial.unique_constraints[0].where[0].op);
    try std.testing.expectEqualStrings("\"active\"", parsed_partial.unique_constraints[0].where[1].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), parsed_partial.unique_constraints[1].expressions.len);
    try std.testing.expectEqual(UniqueExpressionOp.lower, parsed_partial.unique_constraints[1].expressions[0].op);
    try std.testing.expectEqualStrings("email", parsed_partial.unique_constraints[1].expressions[0].field);
    try std.testing.expectEqual(@as(usize, 1), parsed_partial.unique_constraints[2].expressions.len);
    try std.testing.expectEqual(UniqueExpressionOp.upper, parsed_partial.unique_constraints[2].expressions[0].op);
    try std.testing.expectEqualStrings("email", parsed_partial.unique_constraints[2].expressions[0].field);
    try std.testing.expectEqual(@as(usize, 1), parsed_partial.unique_constraints[3].expressions.len);
    try std.testing.expectEqual(UniqueExpressionOp.md5, parsed_partial.unique_constraints[3].expressions[0].op);
    try std.testing.expectEqualStrings("email", parsed_partial.unique_constraints[3].expressions[0].field);
    try std.testing.expectEqual(UniqueConstraintValidationState.enforced, parsed_partial.unique_constraints[0].validation_state);
    try std.testing.expectEqual(UniqueConstraintValidationState.unvalidated, parsed_partial.unique_constraints[1].validation_state);
    try std.testing.expectEqual(UniqueConstraintValidationState.enforced, parsed_partial.unique_constraints[2].validation_state);
    try std.testing.expectEqual(UniqueConstraintValidationState.enforced, parsed_partial.unique_constraints[3].validation_state);
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"keyword\"},\"status\":{\"type\":\"keyword\"}},\"required\":[],\"additionalProperties\":false}}},\"unique_constraints\":[{\"name\":\"bad\",\"columns\":[\"email\"],\"include_columns\":[\"email\"]}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"keyword\"}},\"required\":[],\"additionalProperties\":false}}},\"unique_constraints\":[{\"name\":\"bad\",\"columns\":[\"email\"],\"validation_state\":\"validating\"}]}",
        ),
    );
}

test "relational schema validates expression check types" {
    const valid_schema =
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"fee":{"type":"numeric"},"created_at_ns":{"type":"datetime"},"enabled":{"type":"boolean"},"metadata":{"type":"json"},"tags":{"type":"array","items":{"type":"keyword"}}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"status_case_match","field":"status","op":"eq","value":"OPEN","collation":"antfly.case_insensitive"},{"name":"amount_fee_nonnegative","expression":{"lhs":{"op":"add","args":[{"field":"amount"},{"field":"fee"}]},"op":"gte","rhs":{"value":0}}},{"name":"status_not_deleted","expression":{"lhs":{"op":"lower","args":[{"field":"status"}]},"op":"ne","rhs":{"value":"deleted"}}},{"name":"created_hour_bin_valid","expression":{"lhs":{"op":"date_bin","args":[{"op":"interval_ns","args":[{"value":3600000000000}]},{"field":"created_at_ns"},{"value":0}]},"op":"gte","rhs":{"value":0}}},{"name":"enabled_known","expression":{"lhs":{"field":"enabled"},"op":"is_not_null"}},{"name":"tag_count_nonnegative","expression":{"lhs":{"op":"array_length","args":[{"field":"tags"}]},"op":"gte","rhs":{"value":0}}},{"name":"metadata_source_present","expression":{"lhs":{"op":"json_extract","args":[{"field":"metadata"}],"path":"source","as_text":true},"op":"is_not_null"}}]}
    ;
    var parsed = try parseSchema(std.testing.allocator, valid_schema);
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 7), parsed.checks.len);
    try std.testing.expectEqualStrings("antfly.case_insensitive", parsed.checks[0].collation.?);

    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"amount_text_mismatch","expression":{"lhs":{"field":"amount"},"op":"eq","rhs":{"value":"open"}}}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"numeric_collation","field":"amount","op":"eq","value":1,"collation":"antfly.case_insensitive"}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"expression_collation","expression":{"lhs":{"field":"status"},"op":"eq","rhs":{"value":"open"}},"collation":"antfly.case_insensitive"}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"json_not_orderable","expression":{"lhs":{"field":"metadata"},"op":"gt","rhs":{"value":{"source":"api"}}}}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"enabled":{"type":"boolean"}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"boolean_operand_mismatch","expression":{"lhs":{"op":"and","args":[{"field":"enabled"},{"field":"status"}]},"op":"eq","rhs":{"value":true}}}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"created_at_ns":{"type":"datetime"}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"calendar_bin_mismatch","expression":{"lhs":{"op":"date_bin","args":[{"op":"interval_months","args":[{"value":1}]},{"field":"created_at_ns"},{"value":0}]},"op":"gte","rhs":{"value":0}}}]}
    ));
}

test "relational catalog expressions reject volatile functions" {
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"email_key","columns":["email"],"where_expressions":[{"lhs":{"op":"uuid_v4","args":[]},"op":"is_not_null"}]}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"relational_indexes":[{"name":"status_idx","owner_kind":"relational_column","owner_name":"status","access_method":"scalar_column","columns":["status"],"where_expressions":[{"lhs":{"op":"coalesce","args":[{"op":"uuid_v4","args":[]},{"field":"status"}]},"op":"is_not_null"}]}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(std.testing.allocator,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"checks":[{"name":"volatile_case","expression":{"lhs":{"op":"case","cases":[{"when":{"lhs":{"field":"status"},"op":"eq","rhs":{"value":"open"}},"then":{"op":"uuid_v4","args":[]}}],"else":{"field":"status"}},"op":"is_not_null"}}]}
    ));
}

test "parse dynamic template contract and validate selectors" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"dynamic_templates\":[{\"name\":\"dates\",\"match\":\"*_at\",\"unmatch\":\"skip_*\",\"path_match\":\"meta.*\",\"path_unmatch\":\"meta.private.*\",\"match_mapping_type\":\"date\",\"mapping\":{\"type\":\"datetime\",\"analyzer\":\"keyword\",\"index\":false,\"store\":false,\"doc_values\":true,\"include_in_all\":false}}],\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), parsed.dynamic_templates.len);
    try std.testing.expectEqualStrings("dates", parsed.dynamic_templates[0].name);
    try std.testing.expectEqualStrings("*_at", parsed.dynamic_templates[0].match_pattern.?);
    try std.testing.expectEqualStrings("skip_*", parsed.dynamic_templates[0].unmatch_pattern.?);
    try std.testing.expectEqualStrings("meta.*", parsed.dynamic_templates[0].path_match.?);
    try std.testing.expectEqualStrings("meta.private.*", parsed.dynamic_templates[0].path_unmatch.?);
    try std.testing.expectEqualStrings("date", parsed.dynamic_templates[0].match_mapping_type.?);
    try std.testing.expectEqualStrings("datetime", parsed.dynamic_templates[0].field_type.?);
    try std.testing.expectEqualStrings("keyword", parsed.dynamic_templates[0].analyzer.?);
    try std.testing.expectEqual(false, parsed.dynamic_templates[0].do_index.?);
    try std.testing.expectEqual(false, parsed.dynamic_templates[0].store.?);
    try std.testing.expectEqual(true, parsed.dynamic_templates[0].doc_values.?);
    try std.testing.expectEqual(false, parsed.dynamic_templates[0].include_in_all.?);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta.created_at\":\"2026-01-03T00:00:00Z\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta.skip_created_at\":\"2026-01-03T00:00:00Z\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta.created_at\":\"not-a-date\"}" }}),
    );
}

test "parse explicit field analyzer override" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"string\",\"x-antfly-types\":[\"text\",\"keyword\"],\"x-antfly-analyzer\":\"french\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), parsed.document_schemas.len);
    const title = findDocumentProperty(parsed.document_schemas[0].properties, "title") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), title.antfly_types.len);
    try std.testing.expectEqualStrings("french", title.analyzer.?);
}

test "parse schema-present infer_types dynamic indexing opt-in" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"meta\":{\"type\":\"object\",\"additionalProperties\":true,\"x-antfly-dynamic-indexing\":{\"mode\":\"infer_types\"}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    const meta = findDocumentProperty(parsed.document_schemas[0].properties, "meta") orelse return error.TestExpectedEqual;
    try std.testing.expect(meta.additional_properties_allowed.?);
    try std.testing.expect(meta.dynamic_infer_types);
}

test "reject infer_types dynamic indexing without open additionalProperties" {
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"meta\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"number\"},\"x-antfly-dynamic-indexing\":{\"mode\":\"infer_types\"}}}}}}}",
        ),
    );
}

test "validate nested required fields and array items" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"required\":[\"author\",\"tags\"],\"properties\":{\"author\":{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"text\"},\"active\":{\"type\":\"boolean\"}}},\"tags\":{\"type\":\"array\",\"items\":{\"type\":\"keyword\"}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"author\":{\"name\":\"ann\",\"active\":true},\"tags\":[\"a\",\"b\"]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"author\":{\"active\":true},\"tags\":[\"a\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"author\":{\"name\":\"ann\"},\"tags\":[1]}" }}),
    );
}

test "validate enums numeric bounds and anyOf" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\",\"enum\":[\"draft\",\"published\"]},\"score\":{\"type\":\"numeric\",\"minimum\":0,\"maximum\":10},\"metric\":{\"anyOf\":[{\"type\":\"numeric\",\"minimum\":0},{\"type\":\"keyword\",\"enum\":[\"n/a\"]}]}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"status\":\"draft\",\"score\":8,\"metric\":\"n/a\"}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"status\":\"published\",\"score\":0,\"metric\":3}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"status\":\"archived\",\"score\":8,\"metric\":\"n/a\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"status\":\"draft\",\"score\":11,\"metric\":\"n/a\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"status\":\"draft\",\"score\":8,\"metric\":\"bad\"}" }}),
    );
}

test "validate exclusive numeric bounds and multipleOf" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"score\":{\"type\":\"numeric\",\"exclusiveMinimum\":0,\"exclusiveMaximum\":10,\"multipleOf\":0.5}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"score\":5.5}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"score\":0}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"score\":10}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"score\":5.25}" }}),
    );
}

test "validate nullable and type-array fields" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"subtitle\":{\"type\":[\"text\",\"null\"]},\"score\":{\"type\":\"numeric\",\"nullable\":true},\"flag\":{\"type\":[\"boolean\"]}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"subtitle\":null,\"score\":null,\"flag\":true}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"subtitle\":\"beta\",\"score\":1,\"flag\":false}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":null,\"subtitle\":\"beta\",\"score\":1,\"flag\":true}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"subtitle\":\"beta\",\"score\":1,\"flag\":null}" }}),
    );
}

test "validate local defs and refs" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"titleField\":{\"type\":\"text\"},\"metaField\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\"}}},\"scoreField\":{\"type\":\"numeric\",\"nullable\":true}},\"properties\":{\"title\":{\"$ref\":\"#/$defs/titleField\"},\"meta\":{\"$ref\":\"#/$defs/metaField\"},\"score\":{\"$ref\":\"#/$defs/scoreField\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"status\":\"ready\"},\"score\":null}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":1,\"meta\":{\"status\":\"ready\"},\"score\":null}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"status\":1},\"score\":null}" }}),
    );
}

test "validate ref siblings and nested local defs" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"titleField\":{\"type\":\"text\"},\"sharedText\":{\"type\":\"text\",\"minLength\":8}},\"properties\":{\"title\":{\"$ref\":\"#/$defs/titleField\",\"minLength\":3},\"meta\":{\"type\":\"object\",\"$defs\":{\"sharedText\":{\"type\":\"text\",\"minLength\":4}},\"properties\":{\"note\":{\"$ref\":\"#/$defs/sharedText\",\"maxLength\":6}}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"note\":\"short\"}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"ab\",\"meta\":{\"note\":\"short\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"note\":\"abc\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"note\":\"toolong\"}}" }}),
    );
}

test "validate recursive root refs" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"node\",\"enforce_types\":true,\"document_schemas\":{\"node\":{\"schema\":{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"text\"},\"children\":{\"type\":\"array\",\"items\":{\"$ref\":\"#\"}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"name\":\"root\",\"children\":[{\"name\":\"leaf\",\"children\":[]},{\"name\":\"branch\",\"children\":[{\"name\":\"twig\"}]}]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"name\":\"root\",\"children\":[{\"name\":1}]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"name\":\"root\",\"children\":[null]}" }}),
    );
}

test "validate recursive root refs with closure semantics" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"node\",\"enforce_types\":true,\"document_schemas\":{\"node\":{\"schema\":{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"text\"},\"meta\":{\"type\":\"object\",\"allOf\":[{\"patternProperties\":{\"^tag_[a-z]+$\":{\"type\":\"keyword\"}}},{\"properties\":{\"count\":{\"type\":\"numeric\"}}}],\"unevaluatedProperties\":false},\"children\":{\"type\":\"array\",\"items\":{\"$ref\":\"#\"}}},\"unevaluatedProperties\":false}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"count\":2},\"children\":[{\"name\":\"leaf\",\"meta\":{\"tag_kind\":\"leaf\"},\"children\":[]},{\"name\":\"branch\",\"meta\":{\"tag_kind\":\"branch\",\"count\":1},\"children\":[{\"name\":\"twig\",\"meta\":{\"tag_kind\":\"twig\"}}]}]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"count\":2},\"children\":[{\"name\":\"leaf\",\"extra\":\"bad\"}]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"other\":\"bad\"},\"children\":[]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"count\":2},\"children\":[{\"name\":\"leaf\",\"meta\":{\"tag_kind\":1}}]}" }}),
    );
}

test "validate format and additionalItems" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"keyword\",\"format\":\"email\"},\"site\":{\"type\":\"keyword\",\"format\":\"uri\"},\"id\":{\"type\":\"keyword\",\"format\":\"uuid\"},\"coords\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\",\"const\":\"point\"},{\"type\":\"numeric\"}],\"additionalItems\":false},\"labels\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\"}],\"additionalItems\":{\"type\":\"keyword\",\"pattern\":\"^[a-z]+$\"}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\",\"beta\"]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"email\":\"bad\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"email\":\"a@example.com\",\"site\":\"not a uri\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"bad-uuid\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1,2],\"labels\":[\"seed\",\"alpha\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",1]}" }}),
    );
}

test "validate broader string formats" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"published_at\":{\"type\":\"keyword\",\"format\":\"date-time\"},\"birthday\":{\"type\":\"keyword\",\"format\":\"date\"},\"v4\":{\"type\":\"keyword\",\"format\":\"ipv4\"},\"v6\":{\"type\":\"keyword\",\"format\":\"ipv6\"},\"host\":{\"type\":\"keyword\",\"format\":\"hostname\"},\"ref\":{\"type\":\"keyword\",\"format\":\"uri-reference\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"published_at\":\"2024-01-02\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-13-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"999.1.1.1\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"invalid\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"-bad-host\",\"ref\":\"/docs/intro\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs bad\"}" }}),
    );
}

test "validate unevaluated properties and items" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"},\"meta\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}},\"unevaluatedProperties\":{\"type\":\"keyword\"}},\"coords\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\",\"const\":\"point\"}],\"unevaluatedItems\":{\"type\":\"numeric\"}}},\"unevaluatedProperties\":false}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":\"ok\"},\"coords\":[\"point\",1,2]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"extra\":\"bad\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":1},\"coords\":[\"point\",1]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":\"ok\"},\"coords\":[\"point\",\"bad\"]}" }}),
    );
}

test "validate unevaluated composition coverage" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"allOf\":[{\"properties\":{\"kind\":{\"type\":\"keyword\"}}},{\"properties\":{\"meta\":{\"type\":\"object\",\"allOf\":[{\"properties\":{\"title\":{\"type\":\"text\"}}}],\"unevaluatedProperties\":false}}},{\"properties\":{\"coords\":{\"type\":\"array\",\"anyOf\":[{\"prefixItems\":[{\"const\":\"point\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false},{\"prefixItems\":[{\"const\":\"line\"},{\"type\":\"numeric\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false}]}}}],\"unevaluatedProperties\":false}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1]}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"line\",1,2]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"extra\":\"bad\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":\"bad\"},\"coords\":[\"point\",1]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1,2]}" }}),
    );
}

test "validate root unevaluated properties in write loop" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"}},\"unevaluatedProperties\":{\"type\":\"keyword\"}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"ok\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":1}" }}),
    );
}

test "validate conditional and dependency unevaluated coverage" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"}},\"if\":{\"properties\":{\"kind\":{\"const\":\"story\"}}},\"then\":{\"required\":[\"slug\"],\"properties\":{\"slug\":{\"type\":\"keyword\"}}},\"else\":{\"required\":[\"rating\"],\"properties\":{\"rating\":{\"type\":\"numeric\"}}},\"dependentSchemas\":{\"kind\":{\"properties\":{\"details\":{\"type\":\"text\"}}}},\"unevaluatedProperties\":false}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"details\":\"body\"}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"score\",\"rating\":5,\"details\":\"body\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"details\":\"body\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"score\",\"details\":\"body\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"details\":\"body\",\"extra\":\"bad\"}" }}),
    );
}

test "validate anyOf and oneOf branch evaluation coverage" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"}},\"allOf\":[{\"properties\":{\"meta\":{\"type\":\"object\",\"anyOf\":[{\"properties\":{\"mode\":{\"const\":\"alpha\"},\"a\":{\"type\":\"keyword\"}}},{\"properties\":{\"mode\":{\"const\":\"beta\"},\"b\":{\"type\":\"numeric\"}}}],\"unevaluatedProperties\":false}}},{\"properties\":{\"choice\":{\"type\":\"object\",\"oneOf\":[{\"properties\":{\"mode\":{\"const\":\"left\"},\"left\":{\"type\":\"keyword\"}},\"required\":[\"mode\",\"left\"]},{\"properties\":{\"mode\":{\"const\":\"right\"},\"right\":{\"type\":\"numeric\"}},\"required\":[\"mode\",\"right\"]}],\"unevaluatedProperties\":false}}}],\"unevaluatedProperties\":false}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"a\":\"ok\"},\"choice\":{\"mode\":\"left\",\"left\":\"x\"}}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"beta\",\"b\":3},\"choice\":{\"mode\":\"right\",\"right\":9}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"b\":3},\"choice\":{\"mode\":\"left\",\"left\":\"x\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"beta\",\"a\":\"oops\"},\"choice\":{\"mode\":\"right\",\"right\":9}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"a\":\"ok\"},\"choice\":{\"mode\":\"left\",\"right\":9}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"a\":\"ok\",\"extra\":\"bad\"},\"choice\":{\"mode\":\"left\",\"left\":\"x\"}}" }}),
    );
}

test "validate anyOf and oneOf array evaluation coverage" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"coords\":{\"type\":\"array\",\"anyOf\":[{\"minItems\":2,\"prefixItems\":[{\"const\":\"point\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false},{\"minItems\":3,\"prefixItems\":[{\"const\":\"line\"},{\"type\":\"numeric\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false}]},\"choice\":{\"type\":\"array\",\"oneOf\":[{\"minItems\":2,\"prefixItems\":[{\"const\":\"left\"},{\"type\":\"keyword\"}],\"unevaluatedItems\":false},{\"minItems\":2,\"prefixItems\":[{\"const\":\"right\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false}]}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",1],\"choice\":[\"left\",\"ok\"]}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"line\",1,2],\"choice\":[\"right\",9]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",1,2],\"choice\":[\"left\",\"ok\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"line\",1],\"choice\":[\"right\",9]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",1],\"choice\":[\"left\",9]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",1],\"choice\":[\"right\",9,10]}" }}),
    );
}

test "validate composed contains-driven array evaluation coverage" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"series\":{\"type\":\"array\",\"allOf\":[{\"minItems\":2,\"prefixItems\":[{\"const\":\"set\"}]},{\"contains\":{\"type\":\"numeric\",\"minimum\":10},\"minContains\":1}],\"unevaluatedItems\":false},\"selector\":{\"type\":\"array\",\"anyOf\":[{\"contains\":{\"const\":\"hot\"},\"minContains\":1,\"unevaluatedItems\":false},{\"contains\":{\"const\":\"cold\"},\"minContains\":1,\"unevaluatedItems\":false}]},\"exclusive\":{\"type\":\"array\",\"oneOf\":[{\"contains\":{\"const\":\"left\"},\"minContains\":1,\"unevaluatedItems\":false},{\"contains\":{\"const\":\"right\"},\"minContains\":1,\"unevaluatedItems\":false}]}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"series\":[\"set\",10,11],\"selector\":[\"hot\"],\"exclusive\":[\"left\"]}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"series\":[\"set\",12],\"selector\":[\"cold\"],\"exclusive\":[\"right\"]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"series\":[\"set\",10,1],\"selector\":[\"hot\"],\"exclusive\":[\"left\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"series\":[\"set\",12],\"selector\":[\"warm\"],\"exclusive\":[\"left\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"series\":[\"set\",12],\"selector\":[\"hot\",\"cold\"],\"exclusive\":[\"left\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"series\":[\"set\",12],\"selector\":[\"hot\"],\"exclusive\":[\"left\",\"right\"]}" }}),
    );
}

test "validate composed pattern and additional properties evaluation coverage" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"meta\":{\"type\":\"object\",\"allOf\":[{\"patternProperties\":{\"^meta_[a-z]+$\":{\"type\":\"keyword\"}}},{\"properties\":{\"count\":{\"type\":\"numeric\"}}}],\"unevaluatedProperties\":false},\"choice\":{\"type\":\"object\",\"anyOf\":[{\"patternProperties\":{\"^flag_[a-z]+$\":{\"type\":\"boolean\"}}},{\"additionalProperties\":{\"type\":\"numeric\"}}],\"unevaluatedProperties\":false},\"exclusive\":{\"type\":\"object\",\"oneOf\":[{\"patternProperties\":{\"^name_[a-z]+$\":{\"type\":\"text\"}},\"unevaluatedProperties\":false},{\"additionalProperties\":{\"type\":\"numeric\"},\"unevaluatedProperties\":false}],\"unevaluatedProperties\":false}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"score\":7},\"exclusive\":{\"score\":9}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"other\":\"bad\"},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":\"bad\"},\"exclusive\":{\"name_primary\":\"alpha\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":true,\"score\":7},\"exclusive\":{\"name_primary\":\"alpha\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"score\":7},\"exclusive\":{\"name_primary\":\"alpha\",\"score\":9}}" }}),
    );
}

test "validate composed ref closure evaluation coverage" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"meta_patterns\":{\"patternProperties\":{\"^meta_[a-z]+$\":{\"type\":\"keyword\"}}},\"meta_count\":{\"properties\":{\"count\":{\"type\":\"numeric\"}}},\"choice_flags\":{\"patternProperties\":{\"^flag_[a-z]+$\":{\"type\":\"boolean\"}}},\"choice_numbers\":{\"additionalProperties\":{\"type\":\"numeric\"}},\"exclusive_names\":{\"patternProperties\":{\"^name_[a-z]+$\":{\"type\":\"text\"}},\"unevaluatedProperties\":false},\"exclusive_numbers\":{\"additionalProperties\":{\"type\":\"numeric\"},\"unevaluatedProperties\":false}},\"properties\":{\"meta\":{\"type\":\"object\",\"allOf\":[{\"$ref\":\"#/$defs/meta_patterns\"},{\"$ref\":\"#/$defs/meta_count\"}],\"unevaluatedProperties\":false},\"choice\":{\"type\":\"object\",\"anyOf\":[{\"$ref\":\"#/$defs/choice_flags\"},{\"$ref\":\"#/$defs/choice_numbers\"}],\"unevaluatedProperties\":false},\"exclusive\":{\"type\":\"object\",\"oneOf\":[{\"$ref\":\"#/$defs/exclusive_names\"},{\"$ref\":\"#/$defs/exclusive_numbers\"}],\"unevaluatedProperties\":false}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"score\":7},\"exclusive\":{\"score\":9}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"other\":\"bad\"},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":\"bad\"},\"exclusive\":{\"name_primary\":\"alpha\"}}" }}),
    );
}

test "validate nullable composed refs" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"nullable_keyword\":{\"type\":[\"keyword\",\"null\"]},\"null_or_x\":{\"anyOf\":[{\"const\":null},{\"type\":\"keyword\",\"enum\":[\"x\"]}]}} ,\"properties\":{\"maybe\":{\"allOf\":[{\"$ref\":\"#/$defs/nullable_keyword\"},{\"$ref\":\"#/$defs/null_or_x\"}]}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"maybe\":null}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"maybe\":\"x\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"maybe\":\"y\"}" }}),
    );
}

test "validate ttl field values and schema bindings" {
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(std.testing.allocator, "{\"ttl_duration_ns\":1,\"ttl_field\":\"\"}"),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"ttl_duration_ns\":1,\"ttl_field\":\"expires_at\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"expires_at\":{\"type\":\"keyword\"}}}}}}",
        ),
    );

    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"ttl_duration_ns\":1,\"ttl_field\":\"expires_at\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"expires_at\":{\"type\":\"datetime\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"expires_at\":1700000000000000000}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"expires_at\":\"2024-01-02T03:04:05Z\"}" }});
    try std.testing.expectEqual(
        @as(?u64, 1_700_000_000_000_000_000),
        try documentTtlTimestampNs(std.testing.allocator, parsed, "{\"expires_at\":1700000000000000000}"),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"expires_at\":\"not-a-time\"}" }}),
    );
}

test "validate escaped ref tokens and direct fragment refs" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"slash/name\":{\"type\":\"text\"},\"tilde~name\":{\"type\":\"keyword\"}},\"properties\":{\"title\":{\"$ref\":\"#/$defs/slash~1name\"},\"kind\":{\"$ref\":\"#/$defs/tilde~0name\"},\"meta\":{\"type\":\"object\",\"$defs\":{\"local/name\":{\"type\":\"text\"}},\"properties\":{\"note\":{\"$ref\":\"#/properties/meta/$defs/local~1name\"},\"shadow\":{\"$ref\":\"#/properties/title\"}},\"required\":[\"note\",\"shadow\"]}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"kind\":\"ready\",\"meta\":{\"note\":\"short\",\"shadow\":\"again\"}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":1,\"kind\":\"ready\",\"meta\":{\"note\":\"short\",\"shadow\":\"again\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"kind\":true,\"meta\":{\"note\":\"short\",\"shadow\":\"again\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"kind\":\"ready\",\"meta\":{\"note\":\"short\",\"shadow\":1}}" }}),
    );
}

test "validate oneOf allOf pattern and item cardinality" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"sku\":{\"type\":\"keyword\",\"pattern\":\"^[A-Z]{3}-[0-9]{2}$\"},\"tags\":{\"type\":\"array\",\"minItems\":1,\"maxItems\":2,\"items\":{\"type\":\"keyword\"}},\"code\":{\"oneOf\":[{\"type\":\"keyword\",\"enum\":[\"A\"]},{\"type\":\"keyword\",\"enum\":[\"B\"]}]},\"score\":{\"allOf\":[{\"type\":\"numeric\",\"minimum\":0},{\"type\":\"numeric\",\"maximum\":5}]}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\"],\"code\":\"A\",\"score\":4}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"sku\":\"bad\",\"tags\":[\"x\"],\"code\":\"A\",\"score\":4}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"sku\":\"ABC-12\",\"tags\":[],\"code\":\"A\",\"score\":4}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\",\"y\",\"z\"],\"code\":\"A\",\"score\":4}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\"],\"code\":\"C\",\"score\":4}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\"],\"code\":\"A\",\"score\":8}" }}),
    );
}

test "validate string length and object cardinality" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"minProperties\":2,\"maxProperties\":3,\"properties\":{\"title\":{\"type\":\"text\",\"minLength\":3,\"maxLength\":5},\"meta\":{\"type\":\"object\",\"minProperties\":1,\"maxProperties\":2,\"properties\":{\"a\":{\"type\":\"keyword\"},\"b\":{\"type\":\"keyword\"},\"c\":{\"type\":\"keyword\"}}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"a\":\"x\"}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"hi\",\"meta\":{\"a\":\"x\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alphabet\",\"meta\":{\"a\":\"x\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"a\":\"x\",\"b\":\"y\",\"c\":\"z\"}}" }}),
    );
}

test "validate root conditionals not and unique items" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"if\":{\"required\":[\"kind\"],\"properties\":{\"kind\":{\"enum\":[\"story\"]}}},\"then\":{\"required\":[\"headline\"]},\"else\":{\"required\":[\"slug\"]},\"properties\":{\"kind\":{\"type\":\"keyword\",\"enum\":[\"story\",\"note\"]},\"headline\":{\"type\":\"text\"},\"slug\":{\"type\":\"keyword\"},\"tags\":{\"type\":\"array\",\"uniqueItems\":true,\"items\":{\"type\":\"keyword\"}},\"status\":{\"type\":\"keyword\",\"not\":{\"enum\":[\"archived\"]}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"headline\":\"alpha\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"note\",\"slug\":\"alpha\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"note\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"headline\":\"alpha\",\"tags\":[\"a\",\"a\"],\"status\":\"draft\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"headline\":\"alpha\",\"tags\":[\"a\",\"b\"],\"status\":\"archived\"}" }}),
    );
}

test "validate property names and dependent required" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"dependentRequired\":{\"kind\":[\"slug\"]},\"properties\":{\"kind\":{\"type\":\"keyword\"},\"slug\":{\"type\":\"keyword\"},\"attrs\":{\"type\":\"object\",\"propertyNames\":{\"pattern\":\"^meta_[a-z]+$\"}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"attrs\":{\"meta_color\":\"red\"}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"attrs\":{\"meta_color\":\"red\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"slug\":\"alpha\",\"attrs\":{\"bad\":\"red\"}}" }}),
    );
}

test "validate dependent schemas" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"},\"slug\":{\"type\":\"keyword\"},\"details\":{\"type\":\"text\"}},\"dependentSchemas\":{\"kind\":{\"required\":[\"slug\"],\"properties\":{\"kind\":{\"const\":\"story\"},\"slug\":{\"type\":\"keyword\"}}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"details\":\"ok\"}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"details\":\"ok\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"details\":\"ok\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"note\",\"slug\":\"alpha\",\"details\":\"ok\"}" }}),
    );
}

test "validate legacy dependencies keyword" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"},\"slug\":{\"type\":\"keyword\"},\"mode\":{\"type\":\"keyword\"},\"details\":{\"type\":\"text\"}},\"dependencies\":{\"kind\":[\"slug\"],\"mode\":{\"required\":[\"details\"],\"properties\":{\"mode\":{\"const\":\"long\"},\"details\":{\"type\":\"text\"}}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"mode\":\"long\",\"details\":\"ok\"}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"details\":\"ok\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"mode\":\"long\",\"details\":\"ok\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"mode\":\"long\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"mode\":\"short\",\"details\":\"ok\"}" }}),
    );
}

test "validate additional properties" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"title\":{\"type\":\"text\"},\"meta\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"keyword\"}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"a\":\"x\",\"b\":\"y\"}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"body\":\"unexpected\",\"meta\":{\"a\":\"x\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"a\":1}}" }}),
    );
}

test "validate contains and contains cardinality" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tags\":{\"type\":\"array\",\"contains\":{\"type\":\"keyword\",\"const\":\"hot\"},\"minContains\":1,\"maxContains\":2},\"scores\":{\"type\":\"array\",\"contains\":{\"type\":\"numeric\",\"minimum\":10}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"tags\":[\"hot\",\"warm\"],\"scores\":[1,10,20]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"tags\":[\"warm\"],\"scores\":[10]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"tags\":[\"hot\",\"hot\",\"hot\"],\"scores\":[10]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"tags\":[\"hot\"],\"scores\":[1,2,3]}" }}),
    );
}

test "validate prefix items and pattern properties" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"coords\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\",\"const\":\"point\"},{\"type\":\"numeric\"}],\"items\":{\"type\":\"numeric\"}},\"meta\":{\"type\":\"object\",\"patternProperties\":{\"^meta_[a-z]+$\":{\"type\":\"keyword\"},\"^flag_[a-z]+$\":{\"type\":\"boolean\"}},\"additionalProperties\":false}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",1,2,3],\"meta\":{\"meta_color\":\"red\",\"flag_ready\":true}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[1,1,2],\"meta\":{\"meta_color\":\"red\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",\"bad\"],\"meta\":{\"meta_color\":\"red\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",1],\"meta\":{\"meta_color\":1}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",1],\"meta\":{\"other\":\"x\"}}" }}),
    );
}

test "validate generic json schema object" {
    try validateJsonSchemaJson(std.testing.allocator,
        \\{
        \\  "type": "object",
        \\  "required": ["name", "count"],
        \\  "properties": {
        \\    "name": { "type": "string", "pattern": "^[a-z]+$" },
        \\    "count": { "type": "integer", "minimum": 1 }
        \\  },
        \\  "additionalProperties": false
        \\}
    ,
        \\{"name":"alpha","count":2}
    );
}

test "parse relational secondary index lifecycle states" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        token: []const u8,
        expected: storage_schema.RelationalIndexLifecycle,
    }{
        .{ .token = "ready", .expected = .ready },
        .{ .token = "building", .expected = .building },
        .{ .token = "catching_up", .expected = .catching_up },
        .{ .token = "stale", .expected = .stale },
        .{ .token = "rebuild_required", .expected = .rebuild_required },
        .{ .token = "failed", .expected = .failed },
        .{ .token = "invalid", .expected = .invalid },
        .{ .token = "dropping", .expected = .dropping },
    };

    for (cases) |case| {
        const schema_json = try std.fmt.allocPrint(alloc,
            \\{{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"id":{{"type":"keyword"}},"status":{{"type":"keyword"}}}},"required":["id"],"additionalProperties":false}}}}}},"primary_key":{{"columns":["id"]}},"relational_indexes":[{{"name":"status_idx","owner_kind":"relational_column","owner_name":"status","access_method":"scalar_column","columns":["status"],"lifecycle":"{s}","generation":1,"schema_fingerprint":"secondary-index-v1:status"}}]}}
        , .{case.token});
        defer alloc.free(schema_json);
        var parsed = try parseSchema(alloc, schema_json);
        defer parsed.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), parsed.relational_indexes.len);
        try std.testing.expectEqual(case.expected, parsed.relational_indexes[0].lifecycle);
    }

    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"status_idx","owner_kind":"relational_column","owner_name":"status","access_method":"scalar_column","columns":["status"],"lifecycle":"paused","generation":1,"schema_fingerprint":"secondary-index-v1:status"}]}
    ));
}

test "parse relational table-owned algebraic index metadata" {
    const alloc = std.testing.allocator;

    var valid = try parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"row_algebraic_idx","owner_kind":"table","owner_name":"__antfly_table__","access_method":"algebraic_filter","method_config":{"type":"algebraic","derive_from_schema":true},"generation":1,"schema_fingerprint":"secondary-index-v1:row_algebraic"}]}
    );
    defer valid.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), valid.relational_indexes.len);
    try std.testing.expectEqual(storage_schema.RelationalIndexOwnerKind.table, valid.relational_indexes[0].owner_kind);
    try std.testing.expectEqualStrings(storage_schema.relational_table_index_owner_name, valid.relational_indexes[0].owner_name);
    try std.testing.expectEqual(storage_schema.RelationalIndexAccessMethod.algebraic_filter, valid.relational_indexes[0].access_method);
    try std.testing.expectEqual(@as(usize, 0), valid.relational_indexes[0].columns.len);
    try std.testing.expectEqualStrings("{\"type\":\"algebraic\",\"derive_from_schema\":true}", valid.relational_indexes[0].method_config_json.?);

    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"row_algebraic_idx","owner_kind":"table","owner_name":"__antfly_table__","access_method":"algebraic_filter","generation":1,"schema_fingerprint":"secondary-index-v1:row_algebraic"}]}
    ));

    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"row_algebraic_idx","owner_kind":"table","owner_name":"row","access_method":"algebraic_filter","method_config":{"type":"algebraic","derive_from_schema":true},"generation":1,"schema_fingerprint":"secondary-index-v1:row_algebraic"}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"row_scalar_idx","owner_kind":"table","owner_name":"__antfly_table__","access_method":"scalar_column","method_config":{"type":"algebraic","derive_from_schema":true},"generation":1,"schema_fingerprint":"secondary-index-v1:row_scalar"}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"row_algebraic_idx","owner_kind":"table","owner_name":"__antfly_table__","access_method":"algebraic_filter","method_config":{"type":"algebraic","derive_from_schema":false},"generation":1,"schema_fingerprint":"secondary-index-v1:row_algebraic"}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"row_algebraic_idx","owner_kind":"table","owner_name":"__antfly_table__","access_method":"algebraic_filter","method_config":{"type":"algebraic","derive_from_schema":true},"columns":["status"],"generation":1,"schema_fingerprint":"secondary-index-v1:row_algebraic"}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"status_idx","owner_kind":"relational_column","owner_name":"status","access_method":"ordered_tuple","method_config":{"type":"full_text","field":"status"},"columns":["status"],"keys":[{"column":"status"}],"generation":1,"schema_fingerprint":"secondary-index-v1:status"}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"body":{"type":"text"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"body_text_idx","owner_kind":"relational_column","owner_name":"body","access_method":"text_search","method_config":{"type":"full_text","field":"status"},"columns":["body"],"generation":1,"schema_fingerprint":"secondary-index-v1:body"}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"body":{"type":"text"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"body_text_idx","owner_kind":"relational_column","owner_name":"body","access_method":"text_search","method_config":{"type":"full_text","field":"body"},"columns":["status"],"generation":1,"schema_fingerprint":"secondary-index-v1:body"}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"body":{"type":"text"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"body_text_idx","owner_kind":"relational_column","owner_name":"body","access_method":"text_search","columns":["body"],"generation":1,"schema_fingerprint":"secondary-index-v1:body"}]}
    ));
}

test "relational generated index metadata must be ordered tuple catalog entry" {
    const alloc = std.testing.allocator;

    var valid = try parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"email_lc":{"type":"keyword","generated":{"op":"expression","expression":{"op":"lower","args":[{"field":"email"}]}}}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"email_lc_idx","owner_kind":"relational_column","owner_name":"email_lc","access_method":"ordered_tuple","columns":["email_lc"],"generation":7,"schema_fingerprint":"secondary-index-v1:email_lc_idx","keys":[{"column":"email_lc","direction":"desc","nulls":"last"}]}]}
    );
    defer valid.deinit(alloc);
    const generated = valid.document_schemas[0].properties[2];
    try std.testing.expect(generated.generated != null);
    try std.testing.expectEqual(@as(usize, 1), valid.relational_indexes.len);
    try std.testing.expectEqual(storage_schema.RelationalIndexAccessMethod.ordered_tuple, valid.relational_indexes[0].access_method);
    try std.testing.expectEqual(@as(usize, 1), valid.relational_indexes[0].keys.len);
    try std.testing.expectEqualStrings("email_lc", valid.relational_indexes[0].keys[0].column);
    try std.testing.expectEqual(storage_schema.RelationalIndexKeyDirection.desc, valid.relational_indexes[0].keys[0].direction);
    try std.testing.expectEqual(storage_schema.RelationalIndexKeyNulls.last, valid.relational_indexes[0].keys[0].nulls);

    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"email_lc":{"type":"keyword","generated":{"op":"expression","expression":{"op":"lower","args":[{"field":"email"}]}}}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"email_lc_idx","owner_kind":"relational_column","owner_name":"email_lc","access_method":"scalar_column","columns":["email_lc"],"generation":7,"schema_fingerprint":"secondary-index-v1:email_lc_idx"}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"email_lc":{"type":"keyword","generated":{"op":"expression","expression":{"op":"lower","args":[{"field":"email"}]}}}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"email_lc_idx","owner_kind":"relational_column","owner_name":"email_lc","access_method":"ordered_tuple","columns":["email_lc"],"generation":7,"schema_fingerprint":"secondary-index-v1:email_lc_idx"}]}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"email_lc":{"type":"keyword","generated":{"op":"expression","expression":{"op":"lower","args":[{"field":"email"}]}}}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"email_lc_idx","owner_kind":"relational_column","owner_name":"email_lc","access_method":"ordered_tuple","columns":["email_lc"],"generation":7,"schema_fingerprint":"secondary-index-v1:email_lc_idx"}]}
    ));
}

test "validate generic json schema rejects integer mismatch" {
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateJsonSchemaJson(std.testing.allocator,
            \\{
            \\  "type": "object",
            \\  "properties": {
            \\    "count": { "type": "integer" }
            \\  }
            \\}
        ,
            \\{"count":2.5}
        ),
    );
}
