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

//! Schema management: TableSchema, DocumentSchema, field type validation.
//!
//! Matches Go antfly's lib/schema/ types:
//!   - AntflyType: text, keyword, numeric, embedding, link, boolean, datetime, geopoint, etc.
//!   - FieldMapping: type + index/store/doc_values/analyzer settings
//!   - DynamicTemplate: glob-based pattern matching for field names
//!   - TableSchema: version, TTL config, default type, dynamic templates

const std = @import("std");
const Allocator = std.mem.Allocator;
const backend_erased = @import("backend_erased.zig");
const backend_scan = @import("backend_scan.zig");
const docstore = @import("docstore.zig");
const DocStore = docstore.DocStore;
const lsm_backend = @import("lsm_backend.zig");
const lmdb = @import("lmdb.zig");
const mem_backend = @import("mem_backend.zig");
const platform_time = @import("../platform/time.zig");

fn cleanupTestDir(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
}

var temp_test_path_nonce: u64 = 0;

fn tempTestPath(alloc: Allocator, label: []const u8) ![:0]u8 {
    const nonce = @atomicRmw(u64, &temp_test_path_nonce, .Add, 1, .monotonic);
    const path = try std.fmt.allocPrint(alloc, "/tmp/antfly-{s}-{d}-{d}", .{
        label,
        platform_time.monotonicNs(),
        nonce,
    });
    defer alloc.free(path);
    return try alloc.dupeZ(u8, path);
}

// ============================================================================
// Types
// ============================================================================

pub const AntflyType = enum(u8) {
    text = 0,
    keyword = 1,
    numeric = 2,
    embedding = 3,
    link = 4,
    boolean = 5,
    datetime = 6,
    geopoint = 7,
    geoshape = 8,
    blob = 9,
    html = 10,
    search_as_you_type = 11,
    json = 12,
    array = 13,
};

pub const FieldMapping = struct {
    field_type: AntflyType = .text,
    do_index: bool = true,
    store: bool = true,
    doc_values: bool = false,
    include_in_all: bool = false,
    analyzer: []const u8 = "standard",
};

pub const DynamicTemplate = struct {
    name: []const u8,
    match_pattern: ?[]const u8 = null,
    unmatch_pattern: ?[]const u8 = null,
    path_match: ?[]const u8 = null,
    path_unmatch: ?[]const u8 = null,
    match_mapping_type: ?[]const u8 = null,
    mapping: FieldMapping = .{},
};

pub const FullTextField = struct {
    path: []const u8,
    emitted_name: []const u8,
    analyzer: []const u8,
    include_in_all: bool = false,
};

pub const FullTextDynamicVariant = struct {
    suffix: []const u8,
    analyzer: []const u8,
    include_in_all: bool = false,
};

pub const FullTextDynamicRule = struct {
    parent_path: []const u8,
    segment_pattern: ?[]const u8 = null,
    relative_path: []const u8 = "",
    variants: []const FullTextDynamicVariant = &.{},
};

pub const FullTextDocument = struct {
    name: []const u8,
    fields: []const FullTextField = &.{},
    dynamic_rules: []const FullTextDynamicRule = &.{},
    open_dynamic_paths: []const []const u8 = &.{},
    infer_type_dynamic_paths: []const []const u8 = &.{},
};

/// Storage profile for a table. See zig/RELATIONAL.md.
pub const StorageMode = enum(u8) {
    document = 0,
    relational = 1,
};

pub const ExternalBaseFormat = enum(u8) {
    parquet = 0,
    iceberg = 1,
    lance = 2,
};

pub const ExternalSnapshotMode = union(enum) {
    current,
    snapshot_id: []const u8,
    object_version_digest: []const u8,
};

pub const ExternalCredentialRef = struct {
    ref_id: []const u8,
    scope: []const u8 = "",
};

pub const ExternalWritePolicy = enum(u8) {
    read_only = 0,
    materialized_overlay = 1,
    iceberg_writer = 2,
    lake_native_relational = 3,
};

pub const ExternalBaseSource = struct {
    table_id: []const u8,
    format: ExternalBaseFormat,
    source_uri: []const u8,
    credential_ref: ?ExternalCredentialRef = null,
    snapshot_mode: ExternalSnapshotMode = .current,
    schema_fingerprint: []const u8,
    write_policy: ExternalWritePolicy = .read_only,
};

/// A declared typed column of a relational table. `json` columns
/// (field_type == .json) are indexed as document subtrees; `array` columns keep
/// a first-class relational type while storing canonical array bytes until
/// element-level indexes are declared.
pub const RelationalColumn = struct {
    name: []const u8,
    path: []const u8,
    field_type: AntflyType = .text,
    array_item_type: ?AntflyType = null,
    nullable: bool = true,
    collation: ?[]const u8 = null,
    indexed: bool = true,
    index_lifecycle: RelationalIndexLifecycle = .ready,
    index_generation: u64 = 0,
    index_name: ?[]const u8 = null,
    index_access_method: ?RelationalIndexAccessMethod = null,
    index_schema_fingerprint: ?[]const u8 = null,
    index_include_columns: []const []const u8 = &.{},
    index_keys: []const RelationalIndexKey = &.{},
    default_value: ?RelationalDefaultValue = null,
    on_update_value: ?RelationalDefaultValue = null,
    generated: ?RelationalGeneratedValue = null,
    index_where: []const UniquePredicate = &.{},
    index_where_expressions: []const RelationalRowsExpressionCondition = &.{},
    cardinality_proof: RelationalColumnCardinalityProof = .none,
};

pub const RelationalIndexAccessMethod = enum(u8) {
    scalar_column = 0,
    ordered_tuple = 1,
    algebraic_filter = 2,
    text_search = 3,

    pub fn fromString(text: []const u8) ?RelationalIndexAccessMethod {
        if (std.mem.eql(u8, text, "scalar_column")) return .scalar_column;
        if (std.mem.eql(u8, text, "ordered_tuple")) return .ordered_tuple;
        if (std.mem.eql(u8, text, "algebraic_filter")) return .algebraic_filter;
        if (std.mem.eql(u8, text, "text_search")) return .text_search;
        return null;
    }

    pub fn name(self: RelationalIndexAccessMethod) []const u8 {
        return switch (self) {
            .scalar_column => "scalar_column",
            .ordered_tuple => "ordered_tuple",
            .algebraic_filter => "algebraic_filter",
            .text_search => "text_search",
        };
    }
};

pub const RelationalColumnCardinalityProof = enum(u8) {
    none = 0,
    unique = 1,

    pub fn fromString(text: []const u8) ?RelationalColumnCardinalityProof {
        if (std.mem.eql(u8, text, "none")) return .none;
        if (std.mem.eql(u8, text, "unique")) return .unique;
        return null;
    }
};

pub const RelationalIndexLifecycle = enum(u8) {
    ready = 0,
    building = 1,
    invalid = 2,
    dropping = 3,
    catching_up = 4,
    stale = 5,
    rebuild_required = 6,
    failed = 7,
};

pub const RelationalIndexKeyDirection = enum(u8) {
    asc = 0,
    desc = 1,
};

pub const RelationalIndexKeyNulls = enum(u8) {
    default = 0,
    first = 1,
    last = 2,
};

pub const RelationalIndexKey = struct {
    column: []const u8,
    collation: ?[]const u8 = null,
    direction: RelationalIndexKeyDirection = .asc,
    nulls: RelationalIndexKeyNulls = .default,
};

pub const RelationalDefaultKind = enum(u8) {
    literal = 0,
    now_ns = 1,
    uuid_v4 = 2,
    current_date_ns = 3,
    sequence_next = 4,
    scalar_subquery = 5,
};

pub const RelationalDefaultValue = struct {
    kind: RelationalDefaultKind = .literal,
    value_json: []const u8,
};

pub const RelationalGeneratedOp = enum(u8) {
    lower = 0,
    concat = 1,
    upper = 2,
    md5 = 3,
    concat_ws = 4,
    expression = 5,
};

pub const RelationalGeneratedValue = struct {
    op: RelationalGeneratedOp,
    field: ?[]const u8 = null,
    fields: []const []const u8 = &.{},
    separator: []const u8 = "",
    expression: ?RelationalRowsExpression = null,
};

pub const RelationalCheckOp = enum(u8) {
    is_null = 0,
    is_not_null = 1,
    eq = 2,
    ne = 3,
    gt = 4,
    gte = 5,
    lt = 6,
    lte = 7,
    is_distinct = 8,
    is_not_distinct = 9,
};

pub const RelationalCheck = struct {
    name: []const u8,
    field: []const u8 = "",
    op: RelationalCheckOp = .eq,
    value_json: ?[]const u8 = null,
    collation: ?[]const u8 = null,
    validation_state: RelationalCheckValidationState = .enforced,
    expression: ?RelationalRowsExpressionCondition = null,
};

pub const RelationalCheckValidationState = enum(u8) {
    enforced = 0,
    unvalidated = 1,
    validating = 2,
    invalid = 3,
};

pub const RelationalRowsExpressionKind = enum {
    field,
    value,
    coalesce,
    now,
    lower,
    upper,
    initcap,
    trim,
    ltrim,
    rtrim,
    replace,
    translate,
    substring,
    overlay,
    split_part,
    strpos,
    left,
    right,
    lpad,
    rpad,
    repeat,
    reverse,
    starts_with,
    ends_with,
    ascii,
    chr,
    md5,
    like,
    ilike,
    bool_and,
    bool_or,
    bool_not,
    concat,
    concat_ws,
    length,
    octet_length,
    bit_length,
    nullif,
    greatest,
    least,
    abs,
    round,
    trunc,
    floor,
    ceil,
    sqrt,
    sign,
    power,
    add,
    sub,
    mul,
    div,
    mod,
    interval_ns,
    interval_months,
    date_trunc,
    date_bin,
    date_part,
    case,
    cast,
    json_extract,
    json_typeof,
    json_array_length,
    array_length,
    array_position,
    array_positions,
    array_append,
    array_prepend,
    array_cat,
    array_remove,
    array_replace,
    array_to_string,
    string_to_array,
    uuid_v4,
    json_build_object,
    to_jsonb,
    json_path_exists,
    regexp_replace,
    regexp_match,
    regexp_count,
    regexp_instr,
    regexp_substr,
    soundex,
};

pub const RelationalRowsExpressionFieldSource = enum {
    row,
    existing,
    proposed,
    source,
};

pub const RelationalRowsExpressionCastType = enum {
    text,
    numeric,
    bool,
    datetime,
};

pub const RelationalRowsExpressionCondition = struct {
    lhs: RelationalRowsExpression,
    op: RelationalCheckOp,
    rhs: []const RelationalRowsExpression = &.{},
};

pub const RelationalRowsExpressionPredicateGroup = struct {
    conditions: []const RelationalRowsExpressionCondition = &.{},
};

pub const RelationalRowsExpressionArrayContainsPredicate = struct {
    expression: RelationalRowsExpression,
    value_json: []const u8,
};

pub const RelationalRowsExpressionCaseBranch = struct {
    when: RelationalRowsExpressionCondition,
    then: RelationalRowsExpression,
};

pub const RelationalRowsExpression = struct {
    kind: RelationalRowsExpressionKind,
    field: []const u8 = "",
    field_source: RelationalRowsExpressionFieldSource = .row,
    value_json: []const u8 = "",
    json_path: []const u8 = "",
    json_as_text: bool = false,
    operands: []const RelationalRowsExpression = &.{},
    cast_type: ?RelationalRowsExpressionCastType = null,
    case_branches: []const RelationalRowsExpressionCaseBranch = &.{},
    case_else: []const RelationalRowsExpression = &.{},
};

pub const RelationalRowsExpressionProjection = struct {
    output: []const u8,
    expression: RelationalRowsExpression,
};

pub const RelationalRowsExpressionAssignment = struct {
    field: []const u8,
    expression: RelationalRowsExpression,
};

pub const ForeignKeyAction = enum(u8) {
    restrict = 0,
    set_null = 1,
    cascade = 2,
    no_action = 3,
};

pub const ForeignKeyTiming = enum(u8) {
    immediate = 0,
    deferred = 1,
};

pub const ForeignKeyMatch = enum(u8) {
    simple = 0,
    full = 1,
    partial = 2,
};

pub const ForeignKeyValidationState = enum(u8) {
    enforced = 0,
    unvalidated = 1,
    validating = 2,
    invalid = 3,
};

pub const ForeignKey = struct {
    name: []const u8,
    child_columns: []const []const u8 = &.{},
    child_period: ?[]const u8 = null,
    parent_table: []const u8,
    parent_columns: []const []const u8 = &.{},
    parent_period: ?[]const u8 = null,
    on_delete: ForeignKeyAction = .restrict,
    on_update: ForeignKeyAction = .restrict,
    timing: ForeignKeyTiming = .immediate,
    deferrable: bool = false,
    match: ForeignKeyMatch = .simple,
    validation_state: ForeignKeyValidationState = .enforced,
};

pub const UniqueConstraint = struct {
    name: []const u8,
    columns: []const []const u8 = &.{},
    expressions: []const UniqueExpression = &.{},
    include_columns: []const []const u8 = &.{},
    without_overlaps_period: ?[]const u8 = null,
    nulls_not_distinct: bool = false,
    deferrable: bool = false,
    timing: ForeignKeyTiming = .immediate,
    where: []const UniquePredicate = &.{},
    where_expressions: []const RelationalRowsExpressionCondition = &.{},
    validation_state: UniqueConstraintValidationState = .enforced,
};

pub const RelationalIndexOwnerKind = enum(u8) {
    relational_column = 0,
    unique_constraint = 1,
    table = 2,
};

pub const relational_table_index_owner_name = "__antfly_table__";

pub const RelationalIndexOwnerRange = struct {
    start: []const u8 = "",
    end: []const u8 = "",
    range_id: ?[]const u8 = null,
    placement_generation: u64 = 0,
};

pub const RelationalIndexGenerationRecord = struct {
    generation: u64,
    owner_ranges: []const RelationalIndexOwnerRange = &.{},
    lifecycle: RelationalIndexLifecycle = .ready,
    lag: u64 = 0,
    failure_reason: ?[]const u8 = null,
    ready_watermark: u64 = 0,
};

pub const RelationalIndexPlannerCapabilities = struct {
    equality: bool = false,
    range: bool = false,
    ordering: bool = false,
    prefix: bool = false,
    full_text: bool = false,
    array: bool = false,
    json: bool = false,
    covering: bool = false,
    rank: bool = false,
    algebraic_dictionary: bool = false,
    algebraic_fact: bool = false,
    algebraic_path: bool = false,
};

pub const RelationalIndex = struct {
    name: []const u8,
    owner_kind: RelationalIndexOwnerKind,
    owner_name: []const u8,
    access_method: RelationalIndexAccessMethod,
    method_config_json: ?[]const u8 = null,
    unique: bool = false,
    columns: []const []const u8 = &.{},
    expressions: []const UniqueExpression = &.{},
    include_columns: []const []const u8 = &.{},
    keys: []const RelationalIndexKey = &.{},
    lifecycle: RelationalIndexLifecycle = .ready,
    generation: u64 = 0,
    schema_fingerprint: ?[]const u8 = null,
    owner_ranges: []const RelationalIndexOwnerRange = &.{},
    generation_record: ?RelationalIndexGenerationRecord = null,
    planner_capabilities: RelationalIndexPlannerCapabilities = .{},
    where: []const UniquePredicate = &.{},
    where_expressions: []const RelationalRowsExpressionCondition = &.{},
};

pub fn relationalIndexGenerationRecordValid(index: RelationalIndex) bool {
    switch (index.access_method) {
        .scalar_column => return index.generation_record == null,
        .ordered_tuple, .text_search, .algebraic_filter => {
            const record = index.generation_record orelse return false;
            if (index.generation == 0 or record.generation != index.generation) return false;
            if (record.lifecycle != index.lifecycle) return false;
            if (index.owner_ranges.len != 0 and !relationalIndexOwnerRangeSlicesEqual(index.owner_ranges, record.owner_ranges)) return false;
            return true;
        },
    }
}

pub fn relationalIndexLifecycle(index: RelationalIndex) ?RelationalIndexLifecycle {
    return switch (index.access_method) {
        .scalar_column => index.lifecycle,
        .ordered_tuple, .text_search, .algebraic_filter => if (relationalIndexGenerationRecordValid(index)) index.generation_record.?.lifecycle else null,
    };
}

pub fn relationalIndexQueryReady(index: RelationalIndex) bool {
    if (!relationalIndexGenerationRecordValid(index)) return false;
    const lifecycle = relationalIndexLifecycle(index) orelse return false;
    if (lifecycle != .ready) return false;
    if (index.generation_record) |record| return record.lag == 0;
    return true;
}

pub fn relationalIndexWriteMaintenanceAllowed(index: RelationalIndex) bool {
    if (!relationalIndexGenerationRecordValid(index)) return false;
    const lifecycle = relationalIndexLifecycle(index) orelse return false;
    return switch (lifecycle) {
        .ready, .building, .catching_up => true,
        .invalid, .dropping, .stale, .rebuild_required, .failed => false,
    };
}

pub fn relationalAccessMethodQueryReady(schema: ?TableSchema, access_method: RelationalIndexAccessMethod, index_name: []const u8) bool {
    const active_schema = schema orelse return true;
    var matched = false;
    for (active_schema.relational_indexes) |index| {
        if (index.access_method != access_method) continue;
        if (!std.mem.eql(u8, index.name, index_name)) continue;
        matched = true;
        if (relationalIndexQueryReady(index)) return true;
    }
    return !matched;
}

pub fn relationalAccessMethodWriteMaintenanceAllowed(schema: ?TableSchema, access_method: RelationalIndexAccessMethod, index_name: []const u8) bool {
    const active_schema = schema orelse return true;
    var matched = false;
    for (active_schema.relational_indexes) |index| {
        if (index.access_method != access_method) continue;
        if (!std.mem.eql(u8, index.name, index_name)) continue;
        matched = true;
        if (relationalIndexWriteMaintenanceAllowed(index)) return true;
    }
    return !matched;
}

pub fn relationalIndexOwnerRangeSlicesEqual(
    lhs: []const RelationalIndexOwnerRange,
    rhs: []const RelationalIndexOwnerRange,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |left, right| {
        if (!std.mem.eql(u8, left.start, right.start)) return false;
        if (!std.mem.eql(u8, left.end, right.end)) return false;
        if (left.placement_generation != right.placement_generation) return false;
        if (left.range_id == null and right.range_id != null) return false;
        if (left.range_id != null and right.range_id == null) return false;
        if (left.range_id) |left_id| {
            if (!std.mem.eql(u8, left_id, right.range_id.?)) return false;
        }
    }
    return true;
}

pub const UniqueConstraintValidationState = enum(u8) {
    enforced = 0,
    unvalidated = 1,
    validating = 2,
    invalid = 3,
};

pub const UniqueExpressionOp = enum(u8) {
    lower = 0,
    upper = 1,
    md5 = 2,
    expression = 3,
};

pub const UniqueExpression = struct {
    op: UniqueExpressionOp,
    field: []const u8 = "",
    expression: ?RelationalRowsExpression = null,
};

pub const UniquePredicateOp = enum(u8) {
    is_null = 0,
    is_not_null = 1,
    eq = 2,
    ne = 3,
};

pub const UniquePredicate = struct {
    field: []const u8,
    op: UniquePredicateOp,
    value_json: ?[]const u8 = null,
};

pub const RelationalPeriod = struct {
    name: []const u8,
    start_column: []const u8,
    end_column: []const u8,
    range_type: ?RelationalPeriodRangeType = null,
};

pub const RelationalPeriodRangeType = enum(u8) {
    numrange,
    daterange,
    tsrange,
    tstzrange,
};

pub const PrimaryKey = struct {
    name: ?[]const u8 = null,
    columns: []const []const u8 = &.{},
    include_columns: []const []const u8 = &.{},
    without_overlaps_period: ?[]const u8 = null,
    deferrable: bool = false,
    timing: ForeignKeyTiming = .immediate,
};

pub fn relationalColumnCatalogsEqual(current: []const RelationalColumn, next: []const RelationalColumn) bool {
    if (current.len != next.len) return false;
    for (current, next) |a, b| {
        if (!std.mem.eql(u8, a.name, b.name)) return false;
        if (!std.mem.eql(u8, a.path, b.path)) return false;
        if (!relationalColumnDefinitionsEqual(a, b)) return false;
    }
    return true;
}

pub fn relationalColumnDefinitionsEqual(a: RelationalColumn, b: RelationalColumn) bool {
    if (a.field_type != b.field_type) return false;
    if (a.array_item_type != b.array_item_type) return false;
    if (a.nullable != b.nullable) return false;
    if (!optionalBytesEqual(a.collation, b.collation)) return false;
    if (!relationalDefaultsEqual(a.default_value, b.default_value)) return false;
    if (!relationalDefaultsEqual(a.on_update_value, b.on_update_value)) return false;
    if (!relationalGeneratedValuesEqual(a.generated, b.generated)) return false;
    if (a.cardinality_proof != b.cardinality_proof) return false;
    return true;
}

pub fn relationalIndexKeySlicesEqual(a: []const RelationalIndexKey, b: []const RelationalIndexKey) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left.column, right.column)) return false;
        if (!optionalBytesEqual(left.collation, right.collation)) return false;
        if (left.direction != right.direction) return false;
        if (left.nulls != right.nulls) return false;
    }
    return true;
}

fn relationalRowsExpressionConditionSlicesEqual(
    a: []const RelationalRowsExpressionCondition,
    b: []const RelationalRowsExpressionCondition,
) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!relationalRowsExpressionConditionsEqual(left, right)) return false;
    }
    return true;
}

fn optionalBytesEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

fn relationalDefaultsEqual(a: ?RelationalDefaultValue, b: ?RelationalDefaultValue) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return a.?.kind == b.?.kind and std.mem.eql(u8, a.?.value_json, b.?.value_json);
}

fn relationalGeneratedValuesEqual(a: ?RelationalGeneratedValue, b: ?RelationalGeneratedValue) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    if (a.?.op != b.?.op) return false;
    if (!optionalStringsEqual(a.?.field, b.?.field)) return false;
    if (!stringSlicesEqual(a.?.fields, b.?.fields)) return false;
    if (!std.mem.eql(u8, a.?.separator, b.?.separator)) return false;
    if (a.?.expression == null and b.?.expression == null) return true;
    if (a.?.expression == null or b.?.expression == null) return false;
    return relationalRowsExpressionsEqual(a.?.expression.?, b.?.expression.?);
}

pub fn relationalCheckCatalogsEqual(current: []const RelationalCheck, next: []const RelationalCheck) bool {
    if (current.len != next.len) return false;
    for (current, next) |a, b| {
        if (!relationalChecksEqual(a, b)) return false;
        if (a.validation_state != b.validation_state) return false;
    }
    return true;
}

pub fn relationalChecksEqual(a: RelationalCheck, b: RelationalCheck) bool {
    return std.mem.eql(u8, a.name, b.name) and
        std.mem.eql(u8, a.field, b.field) and
        a.op == b.op and
        optionalStringsEqual(a.value_json, b.value_json) and
        optionalStringsEqual(a.collation, b.collation) and
        optionalRelationalRowsExpressionConditionsEqual(a.expression, b.expression);
}

fn optionalRelationalRowsExpressionConditionsEqual(
    a: ?RelationalRowsExpressionCondition,
    b: ?RelationalRowsExpressionCondition,
) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return relationalRowsExpressionConditionsEqual(a.?, b.?);
}

fn relationalRowsExpressionConditionsEqual(
    a: RelationalRowsExpressionCondition,
    b: RelationalRowsExpressionCondition,
) bool {
    if (a.op != b.op or a.rhs.len != b.rhs.len) return false;
    if (!relationalRowsExpressionsEqual(a.lhs, b.lhs)) return false;
    for (a.rhs, b.rhs) |left, right| {
        if (!relationalRowsExpressionsEqual(left, right)) return false;
    }
    return true;
}

fn relationalRowsExpressionsEqual(a: RelationalRowsExpression, b: RelationalRowsExpression) bool {
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

pub fn primaryKeyCatalogsEqual(current: ?PrimaryKey, next: ?PrimaryKey) bool {
    if (current == null and next == null) return true;
    if (current == null or next == null) return false;
    return optionalStringsEqual(current.?.name, next.?.name) and
        stringSlicesEqual(current.?.columns, next.?.columns) and
        stringSlicesEqual(current.?.include_columns, next.?.include_columns) and
        optionalStringsEqual(current.?.without_overlaps_period, next.?.without_overlaps_period) and
        current.?.deferrable == next.?.deferrable and
        current.?.timing == next.?.timing;
}

pub fn relationalPeriodCatalogsEqual(current: []const RelationalPeriod, next: []const RelationalPeriod) bool {
    if (current.len != next.len) return false;
    for (current, next) |a, b| {
        if (!std.mem.eql(u8, a.name, b.name)) return false;
        if (!std.mem.eql(u8, a.start_column, b.start_column)) return false;
        if (!std.mem.eql(u8, a.end_column, b.end_column)) return false;
        if (a.range_type != b.range_type) return false;
    }
    return true;
}

pub fn foreignKeyCatalogsEqual(current: []const ForeignKey, next: []const ForeignKey) bool {
    if (current.len != next.len) return false;
    for (current, next) |a, b| {
        if (!std.mem.eql(u8, a.name, b.name)) return false;
        if (!stringSlicesEqual(a.child_columns, b.child_columns)) return false;
        if (!optionalStringsEqual(a.child_period, b.child_period)) return false;
        if (!std.mem.eql(u8, a.parent_table, b.parent_table)) return false;
        if (!stringSlicesEqual(a.parent_columns, b.parent_columns)) return false;
        if (!optionalStringsEqual(a.parent_period, b.parent_period)) return false;
        if (a.on_delete != b.on_delete) return false;
        if (a.on_update != b.on_update) return false;
        if (a.timing != b.timing) return false;
        if (a.deferrable != b.deferrable) return false;
        if (a.match != b.match) return false;
        if (a.validation_state != b.validation_state) return false;
    }
    return true;
}

pub fn uniqueConstraintCatalogsEqual(current: []const UniqueConstraint, next: []const UniqueConstraint) bool {
    if (current.len != next.len) return false;
    for (current, next) |a, b| {
        if (!std.mem.eql(u8, a.name, b.name)) return false;
        if (!stringSlicesEqual(a.columns, b.columns)) return false;
        if (!uniqueExpressionSlicesEqual(a.expressions, b.expressions)) return false;
        if (!stringSlicesEqual(a.include_columns, b.include_columns)) return false;
        if (!optionalStringsEqual(a.without_overlaps_period, b.without_overlaps_period)) return false;
        if (a.nulls_not_distinct != b.nulls_not_distinct) return false;
        if (a.deferrable != b.deferrable) return false;
        if (a.timing != b.timing) return false;
        if (!uniquePredicateSlicesEqual(a.where, b.where)) return false;
        if (!relationalRowsExpressionConditionSlicesEqual(a.where_expressions, b.where_expressions)) return false;
        if (a.validation_state != b.validation_state) return false;
    }
    return true;
}

fn uniqueExpressionSlicesEqual(a: []const UniqueExpression, b: []const UniqueExpression) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (left.op != right.op) return false;
        if (!std.mem.eql(u8, left.field, right.field)) return false;
        if (left.expression == null and right.expression == null) continue;
        if (left.expression == null or right.expression == null) return false;
        if (!relationalRowsExpressionsEqual(left.expression.?, right.expression.?)) return false;
    }
    return true;
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

fn optionalStringsEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |lhs, rhs| {
        if (!std.mem.eql(u8, lhs, rhs)) return false;
    }
    return true;
}

pub const TableSchema = struct {
    version: u32 = 0,
    default_type: []const u8 = "_default",
    ttl_duration_ns: u64 = 0,
    ttl_field: []const u8 = "_timestamp",
    enforce_types: bool = false,
    storage_mode: StorageMode = .document,
    dynamic_templates: []const DynamicTemplate = &.{},
    full_text_documents: []const FullTextDocument = &.{},
    relational_columns: []const RelationalColumn = &.{},
    primary_key: ?PrimaryKey = null,
    periods: []const RelationalPeriod = &.{},
    foreign_keys: []const ForeignKey = &.{},
    unique_constraints: []const UniqueConstraint = &.{},
    relational_indexes: []const RelationalIndex = &.{},
    checks: []const RelationalCheck = &.{},
    external_base_source: ?ExternalBaseSource = null,
    system_versioned: bool = false,
};

// ============================================================================
// Schema storage key
// ============================================================================

const schema_key = "\x00\x00__metadata__:schema";
const schema_version_prefix = "\x00\x00__metadata__:schema_v";
const schema_format_version = 56;

// ============================================================================
// Serialization
// ============================================================================

/// Serialize a TableSchema to bytes. Caller owns the returned slice.
pub fn serializeSchema(alloc: Allocator, schema: TableSchema) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(alloc);

    // Header
    try buf.appendSlice(alloc, "ASCH"); // magic
    try appendU32(&buf, alloc, schema_format_version); // format version
    try appendU32(&buf, alloc, schema.version);
    try appendStr(&buf, alloc, schema.default_type);
    try appendU64(&buf, alloc, schema.ttl_duration_ns);
    try appendStr(&buf, alloc, schema.ttl_field);
    try buf.append(alloc, if (schema.enforce_types) 1 else 0);

    // Dynamic templates
    try appendU32(&buf, alloc, @intCast(schema.dynamic_templates.len));
    for (schema.dynamic_templates) |tmpl| {
        try appendStr(&buf, alloc, tmpl.name);
        try appendOptStr(&buf, alloc, tmpl.match_pattern);
        try appendOptStr(&buf, alloc, tmpl.unmatch_pattern);
        try appendOptStr(&buf, alloc, tmpl.path_match);
        try appendOptStr(&buf, alloc, tmpl.path_unmatch);
        try appendOptStr(&buf, alloc, tmpl.match_mapping_type);
        try buf.append(alloc, @intFromEnum(tmpl.mapping.field_type));
        try buf.append(alloc, if (tmpl.mapping.do_index) 1 else 0);
        try buf.append(alloc, if (tmpl.mapping.store) 1 else 0);
        try buf.append(alloc, if (tmpl.mapping.doc_values) 1 else 0);
        try buf.append(alloc, if (tmpl.mapping.include_in_all) 1 else 0);
        try appendStr(&buf, alloc, tmpl.mapping.analyzer);
    }

    try appendU32(&buf, alloc, @intCast(schema.full_text_documents.len));
    for (schema.full_text_documents) |doc| {
        try appendStr(&buf, alloc, doc.name);
        try appendU32(&buf, alloc, @intCast(doc.fields.len));
        for (doc.fields) |field| {
            try appendStr(&buf, alloc, field.path);
            try appendStr(&buf, alloc, field.emitted_name);
            try appendStr(&buf, alloc, field.analyzer);
            try buf.append(alloc, if (field.include_in_all) 1 else 0);
        }
        try appendU32(&buf, alloc, @intCast(doc.dynamic_rules.len));
        for (doc.dynamic_rules) |rule| {
            try appendStr(&buf, alloc, rule.parent_path);
            try appendOptStr(&buf, alloc, rule.segment_pattern);
            try appendStr(&buf, alloc, rule.relative_path);
            try appendU32(&buf, alloc, @intCast(rule.variants.len));
            for (rule.variants) |variant| {
                try appendStr(&buf, alloc, variant.suffix);
                try appendStr(&buf, alloc, variant.analyzer);
                try buf.append(alloc, if (variant.include_in_all) 1 else 0);
            }
        }
        try appendU32(&buf, alloc, @intCast(doc.open_dynamic_paths.len));
        for (doc.open_dynamic_paths) |path| try appendStr(&buf, alloc, path);
        try appendU32(&buf, alloc, @intCast(doc.infer_type_dynamic_paths.len));
        for (doc.infer_type_dynamic_paths) |path| try appendStr(&buf, alloc, path);
    }

    // Storage mode + relational column catalog.
    try buf.append(alloc, @intFromEnum(schema.storage_mode));
    try appendU32(&buf, alloc, @intCast(schema.relational_columns.len));
    for (schema.relational_columns) |column| {
        try appendStr(&buf, alloc, column.name);
        try appendStr(&buf, alloc, column.path);
        try buf.append(alloc, @intFromEnum(column.field_type));
        if (column.array_item_type) |item_type| {
            try buf.append(alloc, 1);
            try buf.append(alloc, @intFromEnum(item_type));
        } else {
            try buf.append(alloc, 0);
        }
        try buf.append(alloc, if (column.nullable) 1 else 0);
        try appendOptStr(&buf, alloc, column.collation);
        if (column.default_value) |default_value| {
            try buf.append(alloc, 1);
            try buf.append(alloc, @intFromEnum(default_value.kind));
            try appendStr(&buf, alloc, default_value.value_json);
        } else {
            try buf.append(alloc, 0);
        }
        if (column.on_update_value) |on_update_value| {
            try buf.append(alloc, 1);
            try buf.append(alloc, @intFromEnum(on_update_value.kind));
            try appendStr(&buf, alloc, on_update_value.value_json);
        } else {
            try buf.append(alloc, 0);
        }
        if (column.generated) |generated| {
            try buf.append(alloc, 1);
            try buf.append(alloc, @intFromEnum(generated.op));
            try appendOptStr(&buf, alloc, generated.field);
            try appendU32(&buf, alloc, @intCast(generated.fields.len));
            for (generated.fields) |field| try appendStr(&buf, alloc, field);
            try appendStr(&buf, alloc, generated.separator);
            if (generated.expression) |expression| {
                try buf.append(alloc, 1);
                try appendRelationalRowsExpression(&buf, alloc, expression);
            } else {
                try buf.append(alloc, 0);
            }
        } else {
            try buf.append(alloc, 0);
        }
        try buf.append(alloc, @intFromEnum(column.cardinality_proof));
    }

    // Foreign-key catalog (format version 11+).
    try appendU32(&buf, alloc, @intCast(schema.foreign_keys.len));
    for (schema.foreign_keys) |foreign_key| {
        try appendStr(&buf, alloc, foreign_key.name);
        try appendU32(&buf, alloc, @intCast(foreign_key.child_columns.len));
        for (foreign_key.child_columns) |column| try appendStr(&buf, alloc, column);
        try appendOptStr(&buf, alloc, foreign_key.child_period);
        try appendStr(&buf, alloc, foreign_key.parent_table);
        try appendU32(&buf, alloc, @intCast(foreign_key.parent_columns.len));
        for (foreign_key.parent_columns) |column| try appendStr(&buf, alloc, column);
        try appendOptStr(&buf, alloc, foreign_key.parent_period);
        try buf.append(alloc, @intFromEnum(foreign_key.on_delete));
        try buf.append(alloc, @intFromEnum(foreign_key.on_update));
        try buf.append(alloc, @intFromEnum(foreign_key.timing));
        try buf.append(alloc, if (foreign_key.deferrable) 1 else 0);
        try buf.append(alloc, @intFromEnum(foreign_key.match));
        try buf.append(alloc, @intFromEnum(foreign_key.validation_state));
    }

    // Unique-constraint catalog (format version 12+).
    try appendU32(&buf, alloc, @intCast(schema.unique_constraints.len));
    for (schema.unique_constraints) |constraint| {
        try appendStr(&buf, alloc, constraint.name);
        try appendU32(&buf, alloc, @intCast(constraint.columns.len));
        for (constraint.columns) |column| try appendStr(&buf, alloc, column);
        try appendU32(&buf, alloc, @intCast(constraint.expressions.len));
        for (constraint.expressions) |expression| {
            try buf.append(alloc, @intFromEnum(expression.op));
            try appendStr(&buf, alloc, expression.field);
            if (expression.expression) |row_expression| {
                try buf.append(alloc, 1);
                try appendRelationalRowsExpression(&buf, alloc, row_expression);
            } else {
                try buf.append(alloc, 0);
            }
        }
        try appendStringSlice(&buf, alloc, constraint.include_columns);
        try appendOptStr(&buf, alloc, constraint.without_overlaps_period);
        try buf.append(alloc, if (constraint.nulls_not_distinct) 1 else 0);
        try appendU32(&buf, alloc, @intCast(constraint.where.len));
        for (constraint.where) |predicate| {
            try buf.append(alloc, @intFromEnum(predicate.op));
            try appendStr(&buf, alloc, predicate.field);
            try appendOptStr(&buf, alloc, predicate.value_json);
        }
        try appendRelationalRowsExpressionConditionSlice(&buf, alloc, constraint.where_expressions);
        try buf.append(alloc, @intFromEnum(constraint.validation_state));
        try buf.append(alloc, if (constraint.deferrable) 1 else 0);
        try buf.append(alloc, @intFromEnum(constraint.timing));
    }

    // Primary-key catalog (format version 17+).
    if (schema.primary_key) |primary_key| {
        try buf.append(alloc, 1);
        try appendU32(&buf, alloc, @intCast(primary_key.columns.len));
        for (primary_key.columns) |column| try appendStr(&buf, alloc, column);
        try appendOptStr(&buf, alloc, primary_key.without_overlaps_period);
        try appendOptStr(&buf, alloc, primary_key.name);
        try appendStringSlice(&buf, alloc, primary_key.include_columns);
        try buf.append(alloc, if (primary_key.deferrable) 1 else 0);
        try buf.append(alloc, @intFromEnum(primary_key.timing));
    } else {
        try buf.append(alloc, 0);
    }

    // First-class relational index catalog (format version 49+).
    try appendU32(&buf, alloc, @intCast(schema.relational_indexes.len));
    for (schema.relational_indexes) |index| {
        try appendStr(&buf, alloc, index.name);
        try buf.append(alloc, @intFromEnum(index.owner_kind));
        try appendStr(&buf, alloc, index.owner_name);
        try buf.append(alloc, @intFromEnum(index.access_method));
        try appendOptStr(&buf, alloc, index.method_config_json);
        try buf.append(alloc, if (index.unique) 1 else 0);
        try appendStringSlice(&buf, alloc, index.columns);
        try appendUniqueExpressionSlice(&buf, alloc, index.expressions);
        try appendStringSlice(&buf, alloc, index.include_columns);
        try appendRelationalIndexKeySlice(&buf, alloc, index.keys);
        try buf.append(alloc, @intFromEnum(index.lifecycle));
        try appendU64(&buf, alloc, index.generation);
        try appendOptStr(&buf, alloc, index.schema_fingerprint);
        try appendRelationalIndexOwnerRangeSlice(&buf, alloc, index.owner_ranges);
        try appendRelationalIndexGenerationRecord(&buf, alloc, index.generation_record);
        try appendRelationalIndexPlannerCapabilities(&buf, alloc, index.planner_capabilities);
        try appendUniquePredicateSlice(&buf, alloc, index.where);
        try appendRelationalRowsExpressionConditionSlice(&buf, alloc, index.where_expressions);
    }

    // Relational check catalog (format version 20+).
    try appendU32(&buf, alloc, @intCast(schema.checks.len));
    for (schema.checks) |check| {
        try appendStr(&buf, alloc, check.name);
        try appendStr(&buf, alloc, check.field);
        try buf.append(alloc, @intFromEnum(check.op));
        try appendOptStr(&buf, alloc, check.value_json);
        try appendOptStr(&buf, alloc, check.collation);
        try buf.append(alloc, @intFromEnum(check.validation_state));
        if (check.expression) |expression| {
            try buf.append(alloc, 1);
            try appendRelationalRowsExpressionCondition(&buf, alloc, expression);
        } else {
            try buf.append(alloc, 0);
        }
    }

    // Application-time period catalog (format version 30+).
    try appendU32(&buf, alloc, @intCast(schema.periods.len));
    for (schema.periods) |period| {
        try appendStr(&buf, alloc, period.name);
        try appendStr(&buf, alloc, period.start_column);
        try appendStr(&buf, alloc, period.end_column);
        if (period.range_type) |range_type| {
            try buf.append(alloc, 1);
            try buf.append(alloc, @intFromEnum(range_type));
        } else {
            try buf.append(alloc, 0);
        }
    }

    // External base source binding (format version 38+).
    if (schema.external_base_source) |source| {
        try buf.append(alloc, 1);
        try appendStr(&buf, alloc, source.table_id);
        try buf.append(alloc, @intFromEnum(source.format));
        try appendStr(&buf, alloc, source.source_uri);
        if (source.credential_ref) |credential| {
            try buf.append(alloc, 1);
            try appendStr(&buf, alloc, credential.ref_id);
            try appendStr(&buf, alloc, credential.scope);
        } else {
            try buf.append(alloc, 0);
        }
        switch (source.snapshot_mode) {
            .current => try buf.append(alloc, 0),
            .snapshot_id => |snapshot_id| {
                try buf.append(alloc, 1);
                try appendStr(&buf, alloc, snapshot_id);
            },
            .object_version_digest => |digest| {
                try buf.append(alloc, 2);
                try appendStr(&buf, alloc, digest);
            },
        }
        try appendStr(&buf, alloc, source.schema_fingerprint);
        try buf.append(alloc, @intFromEnum(source.write_policy));
    } else {
        try buf.append(alloc, 0);
    }

    // SQL system-versioned table marker (format version 43+). This durable
    // catalog flag enables transaction-time history capture and native AS-OF
    // read helpers in the relational storage layer.
    try buf.append(alloc, if (schema.system_versioned) 1 else 0);

    const result = try alloc.dupe(u8, buf.items);
    buf.deinit(alloc);
    return result;
}

/// Deserialize a TableSchema from bytes. Dupes all string data so the result
/// is independent of the source buffer. Call `freeSchema` to release.
pub fn deserializeSchema(alloc: Allocator, data: []const u8) !TableSchema {
    if (data.len < 4) return error.InvalidFormat;
    if (!std.mem.eql(u8, data[0..4], "ASCH")) return error.InvalidFormat;

    var pos: usize = 4;
    const fmt_version = readU32(data, &pos);
    if (fmt_version != schema_format_version) return error.UnsupportedVersion;

    const version = readU32(data, &pos);
    const default_type = try alloc.dupe(u8, readStr(data, &pos));
    errdefer alloc.free(default_type);
    const ttl_duration_ns = readU64(data, &pos);
    const ttl_field = try alloc.dupe(u8, readStr(data, &pos));
    errdefer alloc.free(ttl_field);
    const enforce_types = data[pos] == 1;
    pos += 1;

    const num_templates = readU32(data, &pos);
    const templates = try alloc.alloc(DynamicTemplate, num_templates);
    errdefer {
        for (templates[0..num_templates]) |t| {
            alloc.free(t.name);
            if (t.match_pattern) |p| alloc.free(p);
            if (t.unmatch_pattern) |p| alloc.free(p);
            if (t.path_match) |p| alloc.free(p);
            if (t.path_unmatch) |p| alloc.free(p);
            if (t.match_mapping_type) |p| alloc.free(p);
            alloc.free(t.mapping.analyzer);
        }
        alloc.free(templates);
    }

    for (templates) |*tmpl| {
        const name = try alloc.dupe(u8, readStr(data, &pos));
        errdefer alloc.free(name);

        const has_match = data[pos] == 1;
        pos += 1;
        const match_pattern: ?[]const u8 = if (has_match) try alloc.dupe(u8, readStr(data, &pos)) else null;
        errdefer if (match_pattern) |p| alloc.free(p);

        const has_unmatch = if (fmt_version >= 7) data[pos] == 1 else false;
        if (fmt_version >= 7) pos += 1;
        const unmatch_pattern: ?[]const u8 = if (has_unmatch) try alloc.dupe(u8, readStr(data, &pos)) else null;
        errdefer if (unmatch_pattern) |p| alloc.free(p);

        const has_path = data[pos] == 1;
        pos += 1;
        const path_match: ?[]const u8 = if (has_path) try alloc.dupe(u8, readStr(data, &pos)) else null;
        errdefer if (path_match) |p| alloc.free(p);

        const has_path_unmatch = if (fmt_version >= 7) data[pos] == 1 else false;
        if (fmt_version >= 7) pos += 1;
        const path_unmatch: ?[]const u8 = if (has_path_unmatch) try alloc.dupe(u8, readStr(data, &pos)) else null;
        errdefer if (path_unmatch) |p| alloc.free(p);

        const has_match_mapping_type = if (fmt_version >= 7) data[pos] == 1 else false;
        if (fmt_version >= 7) pos += 1;
        const match_mapping_type: ?[]const u8 = if (has_match_mapping_type) try alloc.dupe(u8, readStr(data, &pos)) else null;
        errdefer if (match_mapping_type) |p| alloc.free(p);

        const field_type: AntflyType = @enumFromInt(data[pos]);
        pos += 1;
        const do_index = data[pos] == 1;
        pos += 1;
        const store_val = data[pos] == 1;
        pos += 1;
        const doc_values = data[pos] == 1;
        pos += 1;
        const include_in_all = data[pos] == 1;
        pos += 1;
        const analyzer = try alloc.dupe(u8, readStr(data, &pos));

        tmpl.* = .{
            .name = name,
            .match_pattern = match_pattern,
            .unmatch_pattern = unmatch_pattern,
            .path_match = path_match,
            .path_unmatch = path_unmatch,
            .match_mapping_type = match_mapping_type,
            .mapping = .{
                .field_type = field_type,
                .do_index = do_index,
                .store = store_val,
                .doc_values = doc_values,
                .include_in_all = include_in_all,
                .analyzer = analyzer,
            },
        };
    }

    const full_text_documents: []FullTextDocument = if (fmt_version >= 2) blk: {
        const doc_count = readU32(data, &pos);
        const docs = try alloc.alloc(FullTextDocument, doc_count);
        var docs_initialized: usize = 0;
        errdefer {
            for (docs[0..docs_initialized]) |doc| {
                alloc.free(doc.name);
                for (doc.fields) |field| {
                    alloc.free(field.path);
                    alloc.free(field.emitted_name);
                    alloc.free(field.analyzer);
                }
                if (doc.fields.len > 0) alloc.free(doc.fields);
                for (doc.dynamic_rules) |rule| {
                    alloc.free(rule.parent_path);
                    if (rule.segment_pattern) |pattern| alloc.free(pattern);
                    alloc.free(rule.relative_path);
                    for (rule.variants) |variant| {
                        alloc.free(variant.suffix);
                        alloc.free(variant.analyzer);
                    }
                    if (rule.variants.len > 0) alloc.free(rule.variants);
                }
                if (doc.dynamic_rules.len > 0) alloc.free(doc.dynamic_rules);
                for (doc.open_dynamic_paths) |open_path| alloc.free(open_path);
                if (doc.open_dynamic_paths.len > 0) alloc.free(doc.open_dynamic_paths);
                for (doc.infer_type_dynamic_paths) |infer_path| alloc.free(infer_path);
                if (doc.infer_type_dynamic_paths.len > 0) alloc.free(doc.infer_type_dynamic_paths);
            }
            alloc.free(docs);
        }

        for (docs) |*doc| {
            const name = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(name);

            const field_count = readU32(data, &pos);
            const fields = try alloc.alloc(FullTextField, field_count);
            var fields_initialized: usize = 0;
            errdefer {
                for (fields[0..fields_initialized]) |field| {
                    alloc.free(field.path);
                    alloc.free(field.emitted_name);
                    alloc.free(field.analyzer);
                }
                alloc.free(fields);
            }
            for (fields) |*field| {
                field.* = .{
                    .path = try alloc.dupe(u8, readStr(data, &pos)),
                    .emitted_name = try alloc.dupe(u8, readStr(data, &pos)),
                    .analyzer = try alloc.dupe(u8, readStr(data, &pos)),
                    .include_in_all = data[pos] == 1,
                };
                pos += 1;
                fields_initialized += 1;
            }

            doc.* = .{
                .name = name,
                .fields = fields,
                .dynamic_rules = &.{},
                .open_dynamic_paths = &.{},
                .infer_type_dynamic_paths = &.{},
            };
            if (fmt_version >= 3) {
                const dynamic_rule_count = readU32(data, &pos);
                const dynamic_rules = try alloc.alloc(FullTextDynamicRule, dynamic_rule_count);
                var dynamic_rules_initialized: usize = 0;
                errdefer {
                    for (dynamic_rules[0..dynamic_rules_initialized]) |rule| {
                        alloc.free(rule.parent_path);
                        if (rule.segment_pattern) |pattern| alloc.free(pattern);
                        alloc.free(rule.relative_path);
                        for (rule.variants) |variant| {
                            alloc.free(variant.suffix);
                            alloc.free(variant.analyzer);
                        }
                        if (rule.variants.len > 0) alloc.free(rule.variants);
                    }
                    alloc.free(dynamic_rules);
                }
                for (dynamic_rules) |*rule| {
                    const parent_path = try alloc.dupe(u8, readStr(data, &pos));
                    errdefer alloc.free(parent_path);
                    const has_segment_pattern = if (fmt_version >= 5) data[pos] == 1 else false;
                    if (fmt_version >= 5) pos += 1;
                    const segment_pattern = if (has_segment_pattern)
                        try alloc.dupe(u8, readStr(data, &pos))
                    else
                        null;
                    errdefer if (segment_pattern) |pattern| alloc.free(pattern);
                    const relative_path = if (fmt_version >= 4)
                        try alloc.dupe(u8, readStr(data, &pos))
                    else
                        try alloc.dupe(u8, "");
                    errdefer alloc.free(relative_path);

                    const variant_count = readU32(data, &pos);
                    const variants = try alloc.alloc(FullTextDynamicVariant, variant_count);
                    var variants_initialized: usize = 0;
                    errdefer {
                        for (variants[0..variants_initialized]) |variant| {
                            alloc.free(variant.suffix);
                            alloc.free(variant.analyzer);
                        }
                        alloc.free(variants);
                    }
                    for (variants) |*variant| {
                        variant.* = .{
                            .suffix = try alloc.dupe(u8, readStr(data, &pos)),
                            .analyzer = try alloc.dupe(u8, readStr(data, &pos)),
                            .include_in_all = data[pos] == 1,
                        };
                        pos += 1;
                        variants_initialized += 1;
                    }

                    rule.* = .{
                        .parent_path = parent_path,
                        .segment_pattern = segment_pattern,
                        .relative_path = relative_path,
                        .variants = variants,
                    };
                    dynamic_rules_initialized += 1;
                }
                doc.dynamic_rules = dynamic_rules;
            }
            if (fmt_version >= 6) {
                const open_dynamic_path_count = readU32(data, &pos);
                const open_dynamic_paths = try alloc.alloc([]const u8, open_dynamic_path_count);
                var open_dynamic_paths_initialized: usize = 0;
                errdefer {
                    for (open_dynamic_paths[0..open_dynamic_paths_initialized]) |open_path| alloc.free(open_path);
                    alloc.free(open_dynamic_paths);
                }
                for (open_dynamic_paths) |*open_path| {
                    open_path.* = try alloc.dupe(u8, readStr(data, &pos));
                    open_dynamic_paths_initialized += 1;
                }
                doc.open_dynamic_paths = open_dynamic_paths;
            }
            if (fmt_version >= 8) {
                const infer_type_dynamic_path_count = readU32(data, &pos);
                const infer_type_dynamic_paths = try alloc.alloc([]const u8, infer_type_dynamic_path_count);
                var infer_type_dynamic_paths_initialized: usize = 0;
                errdefer {
                    for (infer_type_dynamic_paths[0..infer_type_dynamic_paths_initialized]) |infer_path| alloc.free(infer_path);
                    alloc.free(infer_type_dynamic_paths);
                }
                for (infer_type_dynamic_paths) |*infer_path| {
                    infer_path.* = try alloc.dupe(u8, readStr(data, &pos));
                    infer_type_dynamic_paths_initialized += 1;
                }
                doc.infer_type_dynamic_paths = infer_type_dynamic_paths;
            }
            docs_initialized += 1;
        }
        break :blk docs;
    } else &.{};
    errdefer freeFullTextDocumentsSlice(alloc, full_text_documents);

    const storage_mode: StorageMode = blk: {
        const mode: StorageMode = @enumFromInt(data[pos]);
        pos += 1;
        break :blk mode;
    };

    const relational_columns: []RelationalColumn = blk: {
        const column_count = readU32(data, &pos);
        const columns = try alloc.alloc(RelationalColumn, column_count);
        var columns_initialized: usize = 0;
        errdefer {
            for (columns[0..columns_initialized]) |column| {
                alloc.free(column.name);
                alloc.free(column.path);
                if (column.collation) |collation| alloc.free(collation);
                freeStringSlice(alloc, column.index_include_columns);
                freeRelationalIndexKeySlice(alloc, column.index_keys);
                if (column.default_value) |value| alloc.free(value.value_json);
                if (column.on_update_value) |value| alloc.free(value.value_json);
                if (column.generated) |value| freeRelationalGeneratedValue(alloc, value);
                freeUniquePredicateSlice(alloc, column.index_where);
                freeRelationalRowsExpressionConditionSlice(alloc, column.index_where_expressions);
            }
            alloc.free(columns);
        }
        for (columns) |*column| {
            const name = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(name);
            const path = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(path);
            const field_type: AntflyType = @enumFromInt(data[pos]);
            pos += 1;
            const array_item_type: ?AntflyType = if (fmt_version >= 21 and data[pos] == 1) item_blk: {
                pos += 1;
                const item_type: AntflyType = @enumFromInt(data[pos]);
                pos += 1;
                break :item_blk item_type;
            } else item_blk: {
                if (fmt_version >= 21) pos += 1;
                break :item_blk null;
            };
            const nullable = data[pos] == 1;
            pos += 1;
            const collation: ?[]const u8 = try readOptStrAlloc(alloc, data, &pos);
            errdefer if (collation) |value| alloc.free(value);
            const default_value: ?RelationalDefaultValue = if (fmt_version >= 20 and data[pos] == 1) default_blk: {
                pos += 1;
                const kind: RelationalDefaultKind = @enumFromInt(data[pos]);
                pos += 1;
                const value_json = try alloc.dupe(u8, readStr(data, &pos));
                break :default_blk .{ .kind = kind, .value_json = value_json };
            } else default_blk: {
                if (fmt_version >= 20) pos += 1;
                break :default_blk null;
            };
            errdefer if (default_value) |value| alloc.free(value.value_json);
            const on_update_value: ?RelationalDefaultValue = if (fmt_version >= 23 and data[pos] == 1) update_blk: {
                pos += 1;
                const kind: RelationalDefaultKind = @enumFromInt(data[pos]);
                pos += 1;
                const value_json = try alloc.dupe(u8, readStr(data, &pos));
                break :update_blk .{ .kind = kind, .value_json = value_json };
            } else update_blk: {
                if (fmt_version >= 23) pos += 1;
                break :update_blk null;
            };
            errdefer if (on_update_value) |value| alloc.free(value.value_json);
            const generated: ?RelationalGeneratedValue = if (fmt_version >= 20 and data[pos] == 1) generated_blk: {
                pos += 1;
                const op: RelationalGeneratedOp = @enumFromInt(data[pos]);
                pos += 1;
                const field = try readOptStrAlloc(alloc, data, &pos);
                errdefer if (field) |value| alloc.free(value);
                const fields = try readStringSliceAlloc(alloc, data, &pos);
                errdefer freeStringSlice(alloc, fields);
                const separator = try alloc.dupe(u8, readStr(data, &pos));
                errdefer alloc.free(separator);
                const expression: ?RelationalRowsExpression = if (fmt_version >= 40 and data[pos] == 1) expression_blk: {
                    pos += 1;
                    break :expression_blk try readRelationalRowsExpressionAlloc(alloc, data, &pos);
                } else expression_blk: {
                    if (fmt_version >= 40) pos += 1;
                    break :expression_blk null;
                };
                errdefer if (expression) |value| freeRelationalRowsExpression(alloc, value);
                break :generated_blk .{ .op = op, .field = field, .fields = fields, .separator = separator, .expression = expression };
            } else generated_blk: {
                if (fmt_version >= 20) pos += 1;
                break :generated_blk null;
            };
            errdefer if (generated) |value| freeRelationalGeneratedValue(alloc, value);
            const cardinality_proof: RelationalColumnCardinalityProof = if (fmt_version >= 46) proof_blk: {
                const value: RelationalColumnCardinalityProof = @enumFromInt(data[pos]);
                pos += 1;
                break :proof_blk value;
            } else .none;
            column.* = .{ .name = name, .path = path, .field_type = field_type, .array_item_type = array_item_type, .nullable = nullable, .collation = collation, .default_value = default_value, .on_update_value = on_update_value, .generated = generated, .cardinality_proof = cardinality_proof };
            columns_initialized += 1;
        }
        break :blk columns;
    };
    errdefer freeRelationalColumnsSlice(alloc, relational_columns);

    const foreign_keys: []ForeignKey = if (fmt_version >= 11) blk: {
        const foreign_key_count = readU32(data, &pos);
        const foreign_keys = try alloc.alloc(ForeignKey, foreign_key_count);
        var foreign_keys_initialized: usize = 0;
        errdefer {
            for (foreign_keys[0..foreign_keys_initialized]) |foreign_key| freeForeignKey(alloc, foreign_key);
            alloc.free(foreign_keys);
        }
        for (foreign_keys) |*foreign_key| {
            const name = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(name);
            const child_columns = try readStringSliceAlloc(alloc, data, &pos);
            errdefer freeStringSlice(alloc, child_columns);
            const child_period = if (fmt_version >= 30) try readOptStrAlloc(alloc, data, &pos) else null;
            errdefer if (child_period) |period| alloc.free(period);
            const parent_table = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(parent_table);
            const parent_columns = try readStringSliceAlloc(alloc, data, &pos);
            errdefer freeStringSlice(alloc, parent_columns);
            const parent_period = if (fmt_version >= 30) try readOptStrAlloc(alloc, data, &pos) else null;
            errdefer if (parent_period) |period| alloc.free(period);
            const on_delete: ForeignKeyAction = @enumFromInt(data[pos]);
            pos += 1;
            const on_update: ForeignKeyAction = if (fmt_version >= 14) update_blk: {
                const value: ForeignKeyAction = @enumFromInt(data[pos]);
                pos += 1;
                break :update_blk value;
            } else .restrict;
            const timing: ForeignKeyTiming = if (fmt_version >= 13) timing_blk: {
                const value: ForeignKeyTiming = @enumFromInt(data[pos]);
                pos += 1;
                break :timing_blk value;
            } else .immediate;
            const deferrable: bool = if (fmt_version >= 16) deferrable_blk: {
                const value = data[pos] == 1;
                pos += 1;
                break :deferrable_blk value;
            } else timing == .deferred;
            const match: ForeignKeyMatch = if (fmt_version >= 15) match_blk: {
                const value: ForeignKeyMatch = @enumFromInt(data[pos]);
                pos += 1;
                break :match_blk value;
            } else .simple;
            const validation_state: ForeignKeyValidationState = if (fmt_version >= 13) state_blk: {
                const value: ForeignKeyValidationState = @enumFromInt(data[pos]);
                pos += 1;
                break :state_blk value;
            } else .enforced;
            foreign_key.* = .{
                .name = name,
                .child_columns = child_columns,
                .child_period = child_period,
                .parent_table = parent_table,
                .parent_columns = parent_columns,
                .parent_period = parent_period,
                .on_delete = on_delete,
                .on_update = on_update,
                .timing = timing,
                .deferrable = deferrable,
                .match = match,
                .validation_state = validation_state,
            };
            foreign_keys_initialized += 1;
        }
        break :blk foreign_keys;
    } else &.{};
    errdefer freeForeignKeysSlice(alloc, foreign_keys);

    const unique_constraints: []UniqueConstraint = if (fmt_version >= 12) blk: {
        const constraint_count = readU32(data, &pos);
        const constraints = try alloc.alloc(UniqueConstraint, constraint_count);
        var constraints_initialized: usize = 0;
        errdefer {
            for (constraints[0..constraints_initialized]) |constraint| freeUniqueConstraint(alloc, constraint);
            alloc.free(constraints);
        }
        for (constraints) |*constraint| {
            const name = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(name);
            const columns = try readStringSliceAlloc(alloc, data, &pos);
            errdefer freeStringSlice(alloc, columns);
            const expressions = if (fmt_version >= 19) try readUniqueExpressionSliceAlloc(alloc, data, &pos, fmt_version) else &.{};
            errdefer freeUniqueExpressionSlice(alloc, expressions);
            const include_columns = if (fmt_version >= 36) try readStringSliceAlloc(alloc, data, &pos) else &.{};
            errdefer freeStringSlice(alloc, include_columns);
            const skip_unique_index_metadata = fmt_version < 51;
            if (skip_unique_index_metadata and fmt_version >= 44) {
                const skipped_index_keys = try readRelationalIndexKeySliceAlloc(alloc, data, &pos);
                freeRelationalIndexKeySlice(alloc, skipped_index_keys);
            }
            const without_overlaps_period = if (fmt_version >= 30) try readOptStrAlloc(alloc, data, &pos) else null;
            errdefer if (without_overlaps_period) |period| alloc.free(period);
            const nulls_not_distinct = if (fmt_version >= 39) nulls_blk: {
                const flag = data[pos] == 1;
                pos += 1;
                break :nulls_blk flag;
            } else false;
            const where = if (fmt_version >= 19) try readUniquePredicateSliceAlloc(alloc, data, &pos) else &.{};
            errdefer freeUniquePredicateSlice(alloc, where);
            const where_expressions = if (fmt_version >= 31) try readRelationalRowsExpressionConditionSliceAlloc(alloc, data, &pos) else &.{};
            errdefer freeRelationalRowsExpressionConditionSlice(alloc, where_expressions);
            const validation_state: UniqueConstraintValidationState = if (fmt_version >= 24) state_blk: {
                const value: UniqueConstraintValidationState = @enumFromInt(data[pos]);
                pos += 1;
                break :state_blk value;
            } else .enforced;
            const deferrable = if (fmt_version >= 42) deferrable_blk: {
                const value = data[pos] == 1;
                pos += 1;
                break :deferrable_blk value;
            } else false;
            const timing: ForeignKeyTiming = if (fmt_version >= 42) timing_blk: {
                const value: ForeignKeyTiming = @enumFromInt(data[pos]);
                pos += 1;
                break :timing_blk value;
            } else .immediate;
            if (skip_unique_index_metadata and fmt_version >= 48) {
                pos += 1;
                _ = readU64(data, &pos);
                if (data[pos] == 1) pos += 2 else pos += 1;
                const skipped_index_schema_fingerprint = try readOptStrAlloc(alloc, data, &pos);
                if (skipped_index_schema_fingerprint) |fingerprint| alloc.free(fingerprint);
            }
            constraint.* = .{
                .name = name,
                .columns = columns,
                .expressions = expressions,
                .include_columns = include_columns,
                .without_overlaps_period = without_overlaps_period,
                .nulls_not_distinct = nulls_not_distinct,
                .deferrable = deferrable,
                .timing = timing,
                .where = where,
                .where_expressions = where_expressions,
                .validation_state = validation_state,
            };
            constraints_initialized += 1;
        }
        break :blk constraints;
    } else &.{};
    errdefer freeUniqueConstraintsSlice(alloc, unique_constraints);

    const primary_key: ?PrimaryKey = if (fmt_version >= 17 and data[pos] == 1) key_blk: {
        pos += 1;
        const columns = try readStringSliceAlloc(alloc, data, &pos);
        errdefer freeStringSlice(alloc, columns);
        const without_overlaps_period = if (fmt_version >= 30) try readOptStrAlloc(alloc, data, &pos) else null;
        errdefer if (without_overlaps_period) |period| alloc.free(period);
        const name = if (fmt_version >= 32) try readOptStrAlloc(alloc, data, &pos) else null;
        errdefer if (name) |value| alloc.free(value);
        const include_columns = if (fmt_version >= 37) try readStringSliceAlloc(alloc, data, &pos) else &.{};
        errdefer freeStringSlice(alloc, include_columns);
        const deferrable = if (fmt_version >= 42) deferrable_blk: {
            const value = data[pos] == 1;
            pos += 1;
            break :deferrable_blk value;
        } else false;
        const timing: ForeignKeyTiming = if (fmt_version >= 42) timing_blk: {
            const value: ForeignKeyTiming = @enumFromInt(data[pos]);
            pos += 1;
            break :timing_blk value;
        } else .immediate;
        break :key_blk .{
            .name = name,
            .columns = columns,
            .include_columns = include_columns,
            .without_overlaps_period = without_overlaps_period,
            .deferrable = deferrable,
            .timing = timing,
        };
    } else key_blk: {
        if (fmt_version >= 17) pos += 1;
        break :key_blk null;
    };
    errdefer if (primary_key) |key| freePrimaryKey(alloc, key);

    const relational_indexes: []RelationalIndex = if (fmt_version >= 49) blk: {
        const index_count = readU32(data, &pos);
        const indexes = try alloc.alloc(RelationalIndex, index_count);
        var indexes_initialized: usize = 0;
        errdefer {
            for (indexes[0..indexes_initialized]) |index| freeRelationalIndex(alloc, index);
            alloc.free(indexes);
        }
        for (indexes) |*index| {
            const name = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(name);
            const owner_kind: RelationalIndexOwnerKind = @enumFromInt(data[pos]);
            pos += 1;
            const owner_name = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(owner_name);
            const access_method: RelationalIndexAccessMethod = @enumFromInt(data[pos]);
            pos += 1;
            const method_config_json = try readOptStrAlloc(alloc, data, &pos);
            errdefer if (method_config_json) |config| alloc.free(config);
            const unique = data[pos] == 1;
            pos += 1;
            const columns = try readStringSliceAlloc(alloc, data, &pos);
            errdefer freeStringSlice(alloc, columns);
            const expressions = try readUniqueExpressionSliceAlloc(alloc, data, &pos, fmt_version);
            errdefer freeUniqueExpressionSlice(alloc, expressions);
            const include_columns = try readStringSliceAlloc(alloc, data, &pos);
            errdefer freeStringSlice(alloc, include_columns);
            const keys = try readRelationalIndexKeySliceAlloc(alloc, data, &pos);
            errdefer freeRelationalIndexKeySlice(alloc, keys);
            const lifecycle: RelationalIndexLifecycle = @enumFromInt(data[pos]);
            pos += 1;
            const generation = readU64(data, &pos);
            const schema_fingerprint = try readOptStrAlloc(alloc, data, &pos);
            errdefer if (schema_fingerprint) |fingerprint| alloc.free(fingerprint);
            const owner_ranges = try readRelationalIndexOwnerRangeSliceAlloc(alloc, data, &pos);
            errdefer freeRelationalIndexOwnerRangeSlice(alloc, owner_ranges);
            const generation_record = try readRelationalIndexGenerationRecordAlloc(alloc, data, &pos);
            errdefer if (generation_record) |record| freeRelationalIndexGenerationRecord(alloc, record);
            const planner_capabilities = readRelationalIndexPlannerCapabilities(data, &pos);
            const where = try readUniquePredicateSliceAlloc(alloc, data, &pos);
            errdefer freeUniquePredicateSlice(alloc, where);
            const where_expressions = try readRelationalRowsExpressionConditionSliceAlloc(alloc, data, &pos);
            errdefer freeRelationalRowsExpressionConditionSlice(alloc, where_expressions);
            index.* = .{
                .name = name,
                .owner_kind = owner_kind,
                .owner_name = owner_name,
                .access_method = access_method,
                .method_config_json = method_config_json,
                .unique = unique,
                .columns = columns,
                .expressions = expressions,
                .include_columns = include_columns,
                .keys = keys,
                .lifecycle = lifecycle,
                .generation = generation,
                .schema_fingerprint = schema_fingerprint,
                .owner_ranges = owner_ranges,
                .generation_record = generation_record,
                .planner_capabilities = planner_capabilities,
                .where = where,
                .where_expressions = where_expressions,
            };
            indexes_initialized += 1;
        }
        break :blk indexes;
    } else &.{};
    errdefer freeRelationalIndexesSlice(alloc, relational_indexes);

    const checks: []RelationalCheck = if (fmt_version >= 20) blk: {
        const check_count = readU32(data, &pos);
        const checks = try alloc.alloc(RelationalCheck, check_count);
        var checks_initialized: usize = 0;
        errdefer {
            for (checks[0..checks_initialized]) |check| freeRelationalCheck(alloc, check);
            alloc.free(checks);
        }
        for (checks) |*check| {
            const name = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(name);
            const field = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(field);
            const op: RelationalCheckOp = @enumFromInt(data[pos]);
            pos += 1;
            const value_json = try readOptStrAlloc(alloc, data, &pos);
            errdefer if (value_json) |value| alloc.free(value);
            const collation = if (fmt_version >= 45) try readOptStrAlloc(alloc, data, &pos) else null;
            errdefer if (collation) |value| alloc.free(value);
            const validation_state: RelationalCheckValidationState = if (fmt_version >= 27) @enumFromInt(data[pos]) else .enforced;
            if (fmt_version >= 27) pos += 1;
            const expression: ?RelationalRowsExpressionCondition = if (fmt_version >= 29 and data[pos] == 1) expression_blk: {
                pos += 1;
                break :expression_blk try readRelationalRowsExpressionConditionAlloc(alloc, data, &pos);
            } else expression_blk: {
                if (fmt_version >= 29) pos += 1;
                break :expression_blk null;
            };
            errdefer if (expression) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
            check.* = .{ .name = name, .field = field, .op = op, .value_json = value_json, .collation = collation, .validation_state = validation_state, .expression = expression };
            checks_initialized += 1;
        }
        break :blk checks;
    } else &.{};
    errdefer freeRelationalChecksSlice(alloc, checks);

    const periods: []RelationalPeriod = if (fmt_version >= 30) blk: {
        const period_count = readU32(data, &pos);
        const periods = try alloc.alloc(RelationalPeriod, period_count);
        var periods_initialized: usize = 0;
        errdefer {
            for (periods[0..periods_initialized]) |period| freeRelationalPeriod(alloc, period);
            alloc.free(periods);
        }
        for (periods) |*period| {
            const name = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(name);
            const start_column = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(start_column);
            const end_column = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(end_column);
            const range_type: ?RelationalPeriodRangeType = if (fmt_version >= 33 and data[pos] == 1) range_blk: {
                pos += 1;
                const raw = data[pos];
                pos += 1;
                break :range_blk switch (raw) {
                    @intFromEnum(RelationalPeriodRangeType.numrange) => .numrange,
                    @intFromEnum(RelationalPeriodRangeType.daterange) => .daterange,
                    @intFromEnum(RelationalPeriodRangeType.tsrange) => .tsrange,
                    @intFromEnum(RelationalPeriodRangeType.tstzrange) => .tstzrange,
                    else => return error.InvalidFormat,
                };
            } else range_blk: {
                if (fmt_version >= 33) pos += 1;
                break :range_blk null;
            };
            period.* = .{ .name = name, .start_column = start_column, .end_column = end_column, .range_type = range_type };
            periods_initialized += 1;
        }
        break :blk periods;
    } else &.{};
    errdefer freeRelationalPeriodsSlice(alloc, periods);

    const external_base_source: ?ExternalBaseSource = if (fmt_version >= 38 and data[pos] == 1) source_blk: {
        pos += 1;
        break :source_blk try readExternalBaseSourceAlloc(alloc, data, &pos);
    } else source_blk: {
        if (fmt_version >= 38) pos += 1;
        break :source_blk null;
    };
    errdefer if (external_base_source) |source| freeExternalBaseSource(alloc, source);
    const system_versioned = if (fmt_version >= 43) system_versioned_blk: {
        const value = data[pos] == 1;
        pos += 1;
        break :system_versioned_blk value;
    } else false;

    return .{
        .version = version,
        .default_type = default_type,
        .ttl_duration_ns = ttl_duration_ns,
        .ttl_field = ttl_field,
        .enforce_types = enforce_types,
        .storage_mode = storage_mode,
        .dynamic_templates = templates,
        .full_text_documents = full_text_documents,
        .relational_columns = relational_columns,
        .primary_key = primary_key,
        .periods = periods,
        .foreign_keys = foreign_keys,
        .unique_constraints = unique_constraints,
        .relational_indexes = relational_indexes,
        .checks = checks,
        .external_base_source = external_base_source,
        .system_versioned = system_versioned,
    };
}

/// Free a schema returned by deserializeSchema.
pub fn freeSchema(alloc: Allocator, s: TableSchema) void {
    alloc.free(s.default_type);
    alloc.free(s.ttl_field);
    for (s.dynamic_templates) |t| {
        alloc.free(t.name);
        if (t.match_pattern) |p| alloc.free(p);
        if (t.unmatch_pattern) |p| alloc.free(p);
        if (t.path_match) |p| alloc.free(p);
        if (t.path_unmatch) |p| alloc.free(p);
        if (t.match_mapping_type) |p| alloc.free(p);
        alloc.free(t.mapping.analyzer);
    }
    if (s.dynamic_templates.len > 0) alloc.free(s.dynamic_templates);
    freeFullTextDocumentsSlice(alloc, s.full_text_documents);
    freeRelationalColumnsSlice(alloc, s.relational_columns);
    if (s.primary_key) |primary_key| freePrimaryKey(alloc, primary_key);
    freeRelationalPeriodsSlice(alloc, s.periods);
    freeForeignKeysSlice(alloc, s.foreign_keys);
    freeUniqueConstraintsSlice(alloc, s.unique_constraints);
    freeRelationalIndexesSlice(alloc, s.relational_indexes);
    freeRelationalChecksSlice(alloc, s.checks);
    if (s.external_base_source) |source| freeExternalBaseSource(alloc, source);
}

pub fn freeExternalBaseSource(alloc: Allocator, source: ExternalBaseSource) void {
    alloc.free(source.table_id);
    alloc.free(source.source_uri);
    if (source.credential_ref) |credential| {
        alloc.free(credential.ref_id);
        alloc.free(credential.scope);
    }
    switch (source.snapshot_mode) {
        .current => {},
        .snapshot_id => |snapshot_id| alloc.free(snapshot_id),
        .object_version_digest => |digest| alloc.free(digest),
    }
    alloc.free(source.schema_fingerprint);
}

fn freeRelationalColumnsSlice(alloc: Allocator, columns: []const RelationalColumn) void {
    for (columns) |column| {
        alloc.free(column.name);
        alloc.free(column.path);
        if (column.collation) |collation| alloc.free(collation);
        if (column.index_name) |index_name| alloc.free(index_name);
        if (column.index_schema_fingerprint) |fingerprint| alloc.free(fingerprint);
        freeStringSlice(alloc, column.index_include_columns);
        freeRelationalIndexKeySlice(alloc, column.index_keys);
        if (column.default_value) |value| alloc.free(value.value_json);
        if (column.on_update_value) |value| alloc.free(value.value_json);
        if (column.generated) |value| freeRelationalGeneratedValue(alloc, value);
        freeUniquePredicateSlice(alloc, column.index_where);
        freeRelationalRowsExpressionConditionSlice(alloc, column.index_where_expressions);
    }
    if (columns.len > 0) alloc.free(columns);
}

fn freeRelationalGeneratedValue(alloc: Allocator, generated: RelationalGeneratedValue) void {
    if (generated.field) |field| alloc.free(field);
    freeStringSlice(alloc, generated.fields);
    alloc.free(generated.separator);
    if (generated.expression) |expression| freeRelationalRowsExpression(alloc, expression);
}

pub fn freeRelationalIndexKeySlice(alloc: Allocator, keys: []const RelationalIndexKey) void {
    for (keys) |key| {
        alloc.free(key.column);
        if (key.collation) |collation| alloc.free(collation);
    }
    if (keys.len > 0) alloc.free(keys);
}

fn freeForeignKeysSlice(alloc: Allocator, foreign_keys: []const ForeignKey) void {
    for (foreign_keys) |foreign_key| freeForeignKey(alloc, foreign_key);
    if (foreign_keys.len > 0) alloc.free(foreign_keys);
}

fn freeForeignKey(alloc: Allocator, foreign_key: ForeignKey) void {
    alloc.free(foreign_key.name);
    freeStringSlice(alloc, foreign_key.child_columns);
    if (foreign_key.child_period) |period| alloc.free(period);
    alloc.free(foreign_key.parent_table);
    freeStringSlice(alloc, foreign_key.parent_columns);
    if (foreign_key.parent_period) |period| alloc.free(period);
}

fn freeRelationalPeriodsSlice(alloc: Allocator, periods: []const RelationalPeriod) void {
    for (periods) |period| freeRelationalPeriod(alloc, period);
    if (periods.len > 0) alloc.free(periods);
}

fn freeRelationalPeriod(alloc: Allocator, period: RelationalPeriod) void {
    alloc.free(period.name);
    alloc.free(period.start_column);
    alloc.free(period.end_column);
}

fn readExternalBaseSourceAlloc(alloc: Allocator, data: []const u8, pos: *usize) !ExternalBaseSource {
    const table_id = try alloc.dupe(u8, readStr(data, pos));
    errdefer alloc.free(table_id);
    const format: ExternalBaseFormat = @enumFromInt(data[pos.*]);
    pos.* += 1;
    const source_uri = try alloc.dupe(u8, readStr(data, pos));
    errdefer alloc.free(source_uri);
    const credential_ref: ?ExternalCredentialRef = if (data[pos.*] == 1) credential_blk: {
        pos.* += 1;
        const ref_id = try alloc.dupe(u8, readStr(data, pos));
        errdefer alloc.free(ref_id);
        const scope = try alloc.dupe(u8, readStr(data, pos));
        errdefer alloc.free(scope);
        break :credential_blk .{ .ref_id = ref_id, .scope = scope };
    } else credential_blk: {
        pos.* += 1;
        break :credential_blk null;
    };
    errdefer if (credential_ref) |credential| {
        alloc.free(credential.ref_id);
        alloc.free(credential.scope);
    };

    const snapshot_mode_tag = data[pos.*];
    pos.* += 1;
    const snapshot_mode: ExternalSnapshotMode = switch (snapshot_mode_tag) {
        0 => .current,
        1 => snapshot_blk: {
            const snapshot_id = try alloc.dupe(u8, readStr(data, pos));
            break :snapshot_blk .{ .snapshot_id = snapshot_id };
        },
        2 => digest_blk: {
            const digest = try alloc.dupe(u8, readStr(data, pos));
            break :digest_blk .{ .object_version_digest = digest };
        },
        else => return error.InvalidFormat,
    };
    errdefer switch (snapshot_mode) {
        .current => {},
        .snapshot_id => |snapshot_id| alloc.free(snapshot_id),
        .object_version_digest => |digest| alloc.free(digest),
    };
    const schema_fingerprint = try alloc.dupe(u8, readStr(data, pos));
    errdefer alloc.free(schema_fingerprint);
    const write_policy: ExternalWritePolicy = @enumFromInt(data[pos.*]);
    pos.* += 1;

    return .{
        .table_id = table_id,
        .format = format,
        .source_uri = source_uri,
        .credential_ref = credential_ref,
        .snapshot_mode = snapshot_mode,
        .schema_fingerprint = schema_fingerprint,
        .write_policy = write_policy,
    };
}

fn freeUniqueConstraintsSlice(alloc: Allocator, constraints: []const UniqueConstraint) void {
    for (constraints) |constraint| freeUniqueConstraint(alloc, constraint);
    if (constraints.len > 0) alloc.free(constraints);
}

fn freeUniqueConstraint(alloc: Allocator, constraint: UniqueConstraint) void {
    alloc.free(constraint.name);
    freeStringSlice(alloc, constraint.columns);
    freeUniqueExpressionSlice(alloc, constraint.expressions);
    freeStringSlice(alloc, constraint.include_columns);
    if (constraint.without_overlaps_period) |period| alloc.free(period);
    freeUniquePredicateSlice(alloc, constraint.where);
    freeRelationalRowsExpressionConditionSlice(alloc, constraint.where_expressions);
}

fn freeRelationalIndexesSlice(alloc: Allocator, indexes: []const RelationalIndex) void {
    for (indexes) |index| freeRelationalIndex(alloc, index);
    if (indexes.len > 0) alloc.free(indexes);
}

fn freeRelationalIndex(alloc: Allocator, index: RelationalIndex) void {
    alloc.free(index.name);
    alloc.free(index.owner_name);
    if (index.method_config_json) |config| alloc.free(config);
    freeStringSlice(alloc, index.columns);
    freeUniqueExpressionSlice(alloc, index.expressions);
    freeStringSlice(alloc, index.include_columns);
    freeRelationalIndexKeySlice(alloc, index.keys);
    if (index.schema_fingerprint) |fingerprint| alloc.free(fingerprint);
    freeRelationalIndexOwnerRangeSlice(alloc, index.owner_ranges);
    if (index.generation_record) |record| freeRelationalIndexGenerationRecord(alloc, record);
    freeUniquePredicateSlice(alloc, index.where);
    freeRelationalRowsExpressionConditionSlice(alloc, index.where_expressions);
}

pub fn freeRelationalIndexGenerationRecord(alloc: Allocator, record: RelationalIndexGenerationRecord) void {
    freeRelationalIndexOwnerRangeSlice(alloc, record.owner_ranges);
    if (record.failure_reason) |reason| alloc.free(reason);
}

fn freeRelationalIndexOwnerRangeSlice(alloc: Allocator, ranges: []const RelationalIndexOwnerRange) void {
    for (ranges) |range| freeRelationalIndexOwnerRange(alloc, range);
    if (ranges.len > 0) alloc.free(ranges);
}

fn freeRelationalIndexOwnerRange(alloc: Allocator, range: RelationalIndexOwnerRange) void {
    alloc.free(range.start);
    alloc.free(range.end);
    if (range.range_id) |range_id| alloc.free(range_id);
}

fn freeUniqueExpressionSlice(alloc: Allocator, expressions: []const UniqueExpression) void {
    for (expressions) |expression| {
        alloc.free(expression.field);
        if (expression.expression) |row_expression| freeRelationalRowsExpression(alloc, row_expression);
    }
    if (expressions.len > 0) alloc.free(expressions);
}

fn freeUniquePredicateSlice(alloc: Allocator, predicates: []const UniquePredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        if (predicate.value_json) |value| alloc.free(value);
    }
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRelationalRowsExpressionConditionSlice(
    alloc: Allocator,
    conditions: []const RelationalRowsExpressionCondition,
) void {
    for (conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
    if (conditions.len > 0) alloc.free(conditions);
}

fn freeRelationalChecksSlice(alloc: Allocator, checks: []const RelationalCheck) void {
    for (checks) |check| freeRelationalCheck(alloc, check);
    if (checks.len > 0) alloc.free(checks);
}

pub fn freeRelationalCheck(alloc: Allocator, check: RelationalCheck) void {
    alloc.free(check.name);
    alloc.free(check.field);
    if (check.value_json) |value| alloc.free(value);
    if (check.collation) |value| alloc.free(value);
    if (check.expression) |expression| freeRelationalRowsExpressionCondition(alloc, expression);
}

pub fn cloneRelationalCheckAlloc(alloc: Allocator, check: RelationalCheck) !RelationalCheck {
    const name = try alloc.dupe(u8, check.name);
    errdefer alloc.free(name);
    const field = try alloc.dupe(u8, check.field);
    errdefer alloc.free(field);
    const value_json = if (check.value_json) |value| try alloc.dupe(u8, value) else null;
    errdefer if (value_json) |value| alloc.free(value);
    const collation = if (check.collation) |value| try alloc.dupe(u8, value) else null;
    errdefer if (collation) |value| alloc.free(value);
    const expression = if (check.expression) |condition| try cloneRelationalRowsExpressionConditionAlloc(alloc, condition) else null;
    errdefer if (expression) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
    return .{
        .name = name,
        .field = field,
        .op = check.op,
        .value_json = value_json,
        .collation = collation,
        .validation_state = check.validation_state,
        .expression = expression,
    };
}

pub fn cloneRelationalRowsExpressionConditionAlloc(
    alloc: Allocator,
    condition: RelationalRowsExpressionCondition,
) anyerror!RelationalRowsExpressionCondition {
    const lhs = try cloneRelationalRowsExpressionAlloc(alloc, condition.lhs);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeRelationalRowsExpression(alloc, lhs);
    const rhs = try alloc.alloc(RelationalRowsExpression, condition.rhs.len);
    var initialized: usize = 0;
    errdefer {
        for (rhs[0..initialized]) |expression| freeRelationalRowsExpression(alloc, expression);
        if (rhs.len > 0) alloc.free(rhs);
    }
    for (condition.rhs) |expression| {
        rhs[initialized] = try cloneRelationalRowsExpressionAlloc(alloc, expression);
        initialized += 1;
    }
    lhs_transferred = true;
    return .{
        .lhs = lhs,
        .op = condition.op,
        .rhs = rhs,
    };
}

pub fn cloneRelationalRowsExpressionAlloc(
    alloc: Allocator,
    expression: RelationalRowsExpression,
) anyerror!RelationalRowsExpression {
    const field = try alloc.dupe(u8, expression.field);
    errdefer alloc.free(field);
    const value_json = try alloc.dupe(u8, expression.value_json);
    errdefer alloc.free(value_json);
    const json_path = try alloc.dupe(u8, expression.json_path);
    errdefer alloc.free(json_path);

    const operands = try alloc.alloc(RelationalRowsExpression, expression.operands.len);
    var operands_initialized: usize = 0;
    errdefer {
        for (operands[0..operands_initialized]) |operand| freeRelationalRowsExpression(alloc, operand);
        if (operands.len > 0) alloc.free(operands);
    }
    for (expression.operands) |operand| {
        operands[operands_initialized] = try cloneRelationalRowsExpressionAlloc(alloc, operand);
        operands_initialized += 1;
    }

    const case_branches = try alloc.alloc(RelationalRowsExpressionCaseBranch, expression.case_branches.len);
    var branches_initialized: usize = 0;
    errdefer {
        for (case_branches[0..branches_initialized]) |branch| freeRelationalRowsExpressionCaseBranch(alloc, branch);
        if (case_branches.len > 0) alloc.free(case_branches);
    }
    for (expression.case_branches) |branch| {
        const when = try cloneRelationalRowsExpressionConditionAlloc(alloc, branch.when);
        var when_transferred = false;
        errdefer if (!when_transferred) freeRelationalRowsExpressionCondition(alloc, when);
        const then = try cloneRelationalRowsExpressionAlloc(alloc, branch.then);
        var then_transferred = false;
        errdefer if (!then_transferred) freeRelationalRowsExpression(alloc, then);
        case_branches[branches_initialized] = .{ .when = when, .then = then };
        when_transferred = true;
        then_transferred = true;
        branches_initialized += 1;
    }

    const case_else = try alloc.alloc(RelationalRowsExpression, expression.case_else.len);
    var else_initialized: usize = 0;
    errdefer {
        for (case_else[0..else_initialized]) |fallback| freeRelationalRowsExpression(alloc, fallback);
        if (case_else.len > 0) alloc.free(case_else);
    }
    for (expression.case_else) |fallback| {
        case_else[else_initialized] = try cloneRelationalRowsExpressionAlloc(alloc, fallback);
        else_initialized += 1;
    }

    return .{
        .kind = expression.kind,
        .field = field,
        .field_source = expression.field_source,
        .value_json = value_json,
        .json_path = json_path,
        .json_as_text = expression.json_as_text,
        .operands = operands,
        .cast_type = expression.cast_type,
        .case_branches = case_branches,
        .case_else = case_else,
    };
}

pub fn freeRelationalRowsExpressionCondition(alloc: Allocator, condition: RelationalRowsExpressionCondition) void {
    freeRelationalRowsExpression(alloc, condition.lhs);
    for (condition.rhs) |rhs| freeRelationalRowsExpression(alloc, rhs);
    if (condition.rhs.len > 0) alloc.free(condition.rhs);
}

fn freeRelationalRowsExpressionCaseBranch(alloc: Allocator, branch: RelationalRowsExpressionCaseBranch) void {
    freeRelationalRowsExpressionCondition(alloc, branch.when);
    freeRelationalRowsExpression(alloc, branch.then);
}

pub fn freeRelationalRowsExpression(alloc: Allocator, expression: RelationalRowsExpression) void {
    alloc.free(expression.field);
    alloc.free(expression.value_json);
    alloc.free(expression.json_path);
    for (expression.operands) |operand| freeRelationalRowsExpression(alloc, operand);
    if (expression.operands.len > 0) alloc.free(expression.operands);
    for (expression.case_branches) |branch| freeRelationalRowsExpressionCaseBranch(alloc, branch);
    if (expression.case_branches.len > 0) alloc.free(expression.case_branches);
    for (expression.case_else) |fallback| freeRelationalRowsExpression(alloc, fallback);
    if (expression.case_else.len > 0) alloc.free(expression.case_else);
}

fn freePrimaryKey(alloc: Allocator, primary_key: PrimaryKey) void {
    if (primary_key.name) |name| alloc.free(name);
    freeStringSlice(alloc, primary_key.columns);
    freeStringSlice(alloc, primary_key.include_columns);
    if (primary_key.without_overlaps_period) |period| alloc.free(period);
}

fn freeStringSlice(alloc: Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

fn freeFullTextDocumentsSlice(alloc: Allocator, docs: []const FullTextDocument) void {
    for (docs) |doc| {
        alloc.free(doc.name);
        for (doc.fields) |field| {
            alloc.free(field.path);
            alloc.free(field.emitted_name);
            alloc.free(field.analyzer);
        }
        if (doc.fields.len > 0) alloc.free(doc.fields);
        for (doc.dynamic_rules) |rule| {
            alloc.free(rule.parent_path);
            if (rule.segment_pattern) |pattern| alloc.free(pattern);
            alloc.free(rule.relative_path);
            for (rule.variants) |variant| {
                alloc.free(variant.suffix);
                alloc.free(variant.analyzer);
            }
            if (rule.variants.len > 0) alloc.free(rule.variants);
        }
        if (doc.dynamic_rules.len > 0) alloc.free(doc.dynamic_rules);
        for (doc.open_dynamic_paths) |open_path| alloc.free(open_path);
        if (doc.open_dynamic_paths.len > 0) alloc.free(doc.open_dynamic_paths);
        for (doc.infer_type_dynamic_paths) |infer_path| alloc.free(infer_path);
        if (doc.infer_type_dynamic_paths.len > 0) alloc.free(doc.infer_type_dynamic_paths);
    }
    if (docs.len > 0) alloc.free(docs);
}

pub const SchemaMetadataPut = struct {
    key: []const u8,
    value: []const u8,
};

/// Save a schema to DocStore.
pub fn saveSchema(store: anytype, alloc: Allocator, schema: TableSchema) !void {
    try saveSchemaWithMetadata(store, alloc, schema, &.{});
}

/// Save a schema and schema-scoped metadata to DocStore in one durable
/// transaction so every local schema surface advances together.
pub fn saveSchemaWithMetadata(store: anytype, alloc: Allocator, schema: TableSchema, metadata_puts: []const SchemaMetadataPut) !void {
    const data = try serializeSchema(alloc, schema);
    defer alloc.free(data);
    const versioned_key = try schemaVersionKeyAlloc(alloc, schema.version);
    defer alloc.free(versioned_key);
    const previous_schema = try loadSchema(store, alloc);
    defer if (previous_schema) |loaded| freeSchema(alloc, loaded);

    const previous_versioned_data = blk: {
        const loaded = previous_schema orelse break :blk null;
        if (loaded.version == schema.version) break :blk null;
        const existing_version = try loadSchemaVersion(store, alloc, loaded.version);
        defer if (existing_version) |existing| freeSchema(alloc, existing);
        if (existing_version != null) break :blk null;
        break :blk try serializeSchema(alloc, loaded);
    };
    defer if (previous_versioned_data) |encoded| alloc.free(encoded);

    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    if (previous_schema) |loaded| {
        if (previous_versioned_data) |encoded| {
            const previous_versioned_key = try schemaVersionKeyAlloc(alloc, loaded.version);
            defer alloc.free(previous_versioned_key);
            try txn.put(previous_versioned_key, encoded);
        }
    }
    try txn.put(schema_key, data);
    try txn.put(versioned_key, data);
    for (metadata_puts) |entry| {
        try txn.put(entry.key, entry.value);
    }
    try txn.commit();
}

/// Load a schema from DocStore. Returns null if no schema exists.
pub fn loadSchema(store: anytype, alloc: Allocator) !?TableSchema {
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const raw = txn.get(schema_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const data = try alloc.dupe(u8, raw);
    defer alloc.free(data);
    return try deserializeSchema(alloc, data);
}

pub fn loadSchemaVersion(store: anytype, alloc: Allocator, version: u32) !?TableSchema {
    const versioned_key = try schemaVersionKeyAlloc(alloc, version);
    defer alloc.free(versioned_key);
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const raw = txn.get(versioned_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const data = try alloc.dupe(u8, raw);
    defer alloc.free(data);
    return try deserializeSchema(alloc, data);
}

pub fn copySchemas(source_store: anytype, dest_store: anytype, alloc: Allocator) !void {
    var source_runtime = try initRuntimeStore(alloc, source_store);
    defer source_runtime.deinit();
    var source_txn = try source_runtime.store.beginProbe();
    defer source_txn.abort();

    var dest_runtime = try initRuntimeStore(alloc, dest_store);
    defer dest_runtime.deinit();
    var dest_txn = try dest_runtime.store.beginWrite();
    errdefer dest_txn.abort();

    if (source_txn.get(schema_key)) |raw| {
        try dest_txn.put(schema_key, raw);
    } else |err| switch (err) {
        error.NotFound => {},
        else => return err,
    }

    const entries = try backend_scan.scanPrefixCurrent(alloc, &source_runtime.store, schema_version_prefix);
    defer backend_scan.freeResults(alloc, entries);
    for (entries) |entry| try dest_txn.put(entry.key, entry.value);

    try dest_txn.commit();
}

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    fn deinit(self: *@This()) void {
        if (self.owned) self.store.deinit();
    }
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = false };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
    }
    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

fn schemaVersionKeyAlloc(alloc: Allocator, version: u32) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{d}", .{ schema_version_prefix, version });
}

// ============================================================================
// Field type resolution
// ============================================================================

/// Resolve the field type for a field/path using dynamic templates without a
/// runtime value. Templates using `match_mapping_type` will not match.
pub fn resolveFieldType(schema: TableSchema, field_name: []const u8) ?FieldMapping {
    return resolveFieldTypeForValue(schema, field_name, null);
}

/// Resolve the field type for a field/path using dynamic templates and an
/// optional runtime value for `match_mapping_type` matching.
pub fn resolveFieldTypeForValue(schema: TableSchema, path: []const u8, value: ?std.json.Value) ?FieldMapping {
    const field_name = fieldNameFromPath(path);
    for (schema.dynamic_templates) |tmpl| {
        if (dynamicTemplateMatches(tmpl, path, field_name, value)) return tmpl.mapping;
    }
    return null;
}

fn dynamicTemplateMatches(
    tmpl: DynamicTemplate,
    path: []const u8,
    field_name: []const u8,
    value: ?std.json.Value,
) bool {
    if (tmpl.match_pattern) |pattern| {
        if (!globMatch(pattern, field_name)) return false;
    }
    if (tmpl.unmatch_pattern) |pattern| {
        if (globMatch(pattern, field_name)) return false;
    }
    if (tmpl.path_match) |pattern| {
        if (!globMatch(pattern, path)) return false;
    }
    if (tmpl.path_unmatch) |pattern| {
        if (globMatch(pattern, path)) return false;
    }
    if (tmpl.match_mapping_type) |expected| {
        const actual = if (value) |v| inferDynamicTemplateMatchType(v) else null;
        if (actual == null or !std.mem.eql(u8, expected, actual.?)) return false;
    }
    return true;
}

/// Public wrapper exposing the canonical `match_mapping_type` inference so other
/// indexes (e.g. the algebraic sidecar) evaluate dynamic-template selectors with
/// identical semantics instead of re-implementing type detection.
pub fn matchMappingTypeName(value: std.json.Value) ?[]const u8 {
    return inferDynamicTemplateMatchType(value);
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

/// Simple glob matching: supports '*' (any chars) and '?' (single char).
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

/// Validate that all field names resolve to known types (when enforce_types=true).
pub fn validateFields(schema: TableSchema, field_names: []const []const u8) !void {
    if (!schema.enforce_types) return;
    for (field_names) |name| {
        if (resolveFieldType(schema, name) == null) {
            return error.UnknownFieldType;
        }
    }
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

fn isValidDate(value: []const u8) bool {
    if (value.len != 10 or value[4] != '-' or value[7] != '-') return false;
    const year = std.fmt.parseInt(i64, value[0..4], 10) catch return false;
    const month = std.fmt.parseInt(i64, value[5..7], 10) catch return false;
    const day = std.fmt.parseInt(i64, value[8..10], 10) catch return false;
    return daysFromCivil(year, month, day) >= 0;
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

// ============================================================================
// Serialization helpers
// ============================================================================

fn appendU32(buf: *std.ArrayListUnmanaged(u8), alloc: Allocator, val: u32) !void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, val, .little);
    try buf.appendSlice(alloc, &bytes);
}

fn appendU64(buf: *std.ArrayListUnmanaged(u8), alloc: Allocator, val: u64) !void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, val, .little);
    try buf.appendSlice(alloc, &bytes);
}

fn appendStr(buf: *std.ArrayListUnmanaged(u8), alloc: Allocator, s: []const u8) !void {
    try appendU32(buf, alloc, @intCast(s.len));
    try buf.appendSlice(alloc, s);
}

fn appendOptStr(buf: *std.ArrayListUnmanaged(u8), alloc: Allocator, s: ?[]const u8) !void {
    if (s) |str| {
        try buf.append(alloc, 1);
        try appendStr(buf, alloc, str);
    } else {
        try buf.append(alloc, 0);
    }
}

fn appendStringSlice(buf: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const []const u8) !void {
    try appendU32(buf, alloc, @intCast(values.len));
    for (values) |value| try appendStr(buf, alloc, value);
}

fn appendRelationalIndexKeySlice(
    buf: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    keys: []const RelationalIndexKey,
) !void {
    try appendU32(buf, alloc, @intCast(keys.len));
    for (keys) |key| {
        try appendStr(buf, alloc, key.column);
        try appendOptStr(buf, alloc, key.collation);
        try buf.append(alloc, @intFromEnum(key.direction));
        try buf.append(alloc, @intFromEnum(key.nulls));
    }
}

fn appendRelationalIndexOwnerRangeSlice(
    buf: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    ranges: []const RelationalIndexOwnerRange,
) !void {
    try appendU32(buf, alloc, @intCast(ranges.len));
    for (ranges) |range| {
        try appendStr(buf, alloc, range.start);
        try appendStr(buf, alloc, range.end);
        try appendOptStr(buf, alloc, range.range_id);
        try appendU64(buf, alloc, range.placement_generation);
    }
}

fn appendRelationalIndexGenerationRecord(
    buf: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    record: ?RelationalIndexGenerationRecord,
) !void {
    if (record) |value| {
        try buf.append(alloc, 1);
        try appendU64(buf, alloc, value.generation);
        try appendRelationalIndexOwnerRangeSlice(buf, alloc, value.owner_ranges);
        try buf.append(alloc, @intFromEnum(value.lifecycle));
        try appendU64(buf, alloc, value.lag);
        try appendOptStr(buf, alloc, value.failure_reason);
        try appendU64(buf, alloc, value.ready_watermark);
    } else {
        try buf.append(alloc, 0);
    }
}

fn appendRelationalIndexPlannerCapabilities(
    buf: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    capabilities: RelationalIndexPlannerCapabilities,
) !void {
    try buf.append(alloc, if (capabilities.equality) 1 else 0);
    try buf.append(alloc, if (capabilities.range) 1 else 0);
    try buf.append(alloc, if (capabilities.ordering) 1 else 0);
    try buf.append(alloc, if (capabilities.prefix) 1 else 0);
    try buf.append(alloc, if (capabilities.full_text) 1 else 0);
    try buf.append(alloc, if (capabilities.array) 1 else 0);
    try buf.append(alloc, if (capabilities.json) 1 else 0);
    try buf.append(alloc, if (capabilities.covering) 1 else 0);
    try buf.append(alloc, if (capabilities.rank) 1 else 0);
    try buf.append(alloc, if (capabilities.algebraic_dictionary) 1 else 0);
    try buf.append(alloc, if (capabilities.algebraic_fact) 1 else 0);
    try buf.append(alloc, if (capabilities.algebraic_path) 1 else 0);
}

fn appendUniqueExpressionSlice(
    buf: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    expressions: []const UniqueExpression,
) !void {
    try appendU32(buf, alloc, @intCast(expressions.len));
    for (expressions) |expression| {
        try buf.append(alloc, @intFromEnum(expression.op));
        try appendStr(buf, alloc, expression.field);
        if (expression.expression) |row_expression| {
            try buf.append(alloc, 1);
            try appendRelationalRowsExpression(buf, alloc, row_expression);
        } else {
            try buf.append(alloc, 0);
        }
    }
}

fn appendUniquePredicateSlice(
    buf: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    predicates: []const UniquePredicate,
) !void {
    try appendU32(buf, alloc, @intCast(predicates.len));
    for (predicates) |predicate| {
        try buf.append(alloc, @intFromEnum(predicate.op));
        try appendStr(buf, alloc, predicate.field);
        try appendOptStr(buf, alloc, predicate.value_json);
    }
}

fn appendRelationalRowsExpressionCondition(
    buf: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    condition: RelationalRowsExpressionCondition,
) anyerror!void {
    try appendRelationalRowsExpression(buf, alloc, condition.lhs);
    try buf.append(alloc, @intFromEnum(condition.op));
    try appendU32(buf, alloc, @intCast(condition.rhs.len));
    for (condition.rhs) |rhs| try appendRelationalRowsExpression(buf, alloc, rhs);
}

fn appendRelationalRowsExpressionConditionSlice(
    buf: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    conditions: []const RelationalRowsExpressionCondition,
) anyerror!void {
    try appendU32(buf, alloc, @intCast(conditions.len));
    for (conditions) |condition| try appendRelationalRowsExpressionCondition(buf, alloc, condition);
}

fn appendRelationalRowsExpression(
    buf: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    expression: RelationalRowsExpression,
) anyerror!void {
    try buf.append(alloc, @intFromEnum(expression.kind));
    try appendStr(buf, alloc, expression.field);
    try buf.append(alloc, @intFromEnum(expression.field_source));
    try appendStr(buf, alloc, expression.value_json);
    try appendStr(buf, alloc, expression.json_path);
    try buf.append(alloc, if (expression.json_as_text) 1 else 0);
    try appendU32(buf, alloc, @intCast(expression.operands.len));
    for (expression.operands) |operand| try appendRelationalRowsExpression(buf, alloc, operand);
    if (expression.cast_type) |cast_type| {
        try buf.append(alloc, 1);
        try buf.append(alloc, @intFromEnum(cast_type));
    } else {
        try buf.append(alloc, 0);
    }
    try appendU32(buf, alloc, @intCast(expression.case_branches.len));
    for (expression.case_branches) |branch| {
        try appendRelationalRowsExpressionCondition(buf, alloc, branch.when);
        try appendRelationalRowsExpression(buf, alloc, branch.then);
    }
    try appendU32(buf, alloc, @intCast(expression.case_else.len));
    for (expression.case_else) |fallback| try appendRelationalRowsExpression(buf, alloc, fallback);
}

fn readOptStrAlloc(alloc: Allocator, data: []const u8, pos: *usize) !?[]const u8 {
    const present = data[pos.*] == 1;
    pos.* += 1;
    if (!present) return null;
    return try alloc.dupe(u8, readStr(data, pos));
}

fn readRelationalRowsExpressionConditionAlloc(
    alloc: Allocator,
    data: []const u8,
    pos: *usize,
) anyerror!RelationalRowsExpressionCondition {
    const lhs = try readRelationalRowsExpressionAlloc(alloc, data, pos);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeRelationalRowsExpression(alloc, lhs);
    const op: RelationalCheckOp = @enumFromInt(data[pos.*]);
    pos.* += 1;
    const rhs_count = readU32(data, pos);
    const rhs = try alloc.alloc(RelationalRowsExpression, rhs_count);
    var rhs_initialized: usize = 0;
    errdefer {
        for (rhs[0..rhs_initialized]) |expression| freeRelationalRowsExpression(alloc, expression);
        if (rhs.len > 0) alloc.free(rhs);
    }
    for (rhs) |*expression| {
        expression.* = try readRelationalRowsExpressionAlloc(alloc, data, pos);
        rhs_initialized += 1;
    }
    lhs_transferred = true;
    return .{ .lhs = lhs, .op = op, .rhs = rhs };
}

fn readRelationalRowsExpressionConditionSliceAlloc(
    alloc: Allocator,
    data: []const u8,
    pos: *usize,
) anyerror![]const RelationalRowsExpressionCondition {
    const count = readU32(data, pos);
    if (count == 0) return &.{};
    const conditions = try alloc.alloc(RelationalRowsExpressionCondition, count);
    var initialized: usize = 0;
    errdefer {
        for (conditions[0..initialized]) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
        alloc.free(conditions);
    }
    for (conditions) |*condition| {
        condition.* = try readRelationalRowsExpressionConditionAlloc(alloc, data, pos);
        initialized += 1;
    }
    return conditions;
}

fn readRelationalRowsExpressionAlloc(
    alloc: Allocator,
    data: []const u8,
    pos: *usize,
) anyerror!RelationalRowsExpression {
    const kind: RelationalRowsExpressionKind = @enumFromInt(data[pos.*]);
    pos.* += 1;
    const field = try alloc.dupe(u8, readStr(data, pos));
    errdefer alloc.free(field);
    const field_source: RelationalRowsExpressionFieldSource = @enumFromInt(data[pos.*]);
    pos.* += 1;
    const value_json = try alloc.dupe(u8, readStr(data, pos));
    errdefer alloc.free(value_json);
    const json_path = try alloc.dupe(u8, readStr(data, pos));
    errdefer alloc.free(json_path);
    const json_as_text = data[pos.*] == 1;
    pos.* += 1;

    const operand_count = readU32(data, pos);
    const operands = try alloc.alloc(RelationalRowsExpression, operand_count);
    var operands_initialized: usize = 0;
    errdefer {
        for (operands[0..operands_initialized]) |operand| freeRelationalRowsExpression(alloc, operand);
        if (operands.len > 0) alloc.free(operands);
    }
    for (operands) |*operand| {
        operand.* = try readRelationalRowsExpressionAlloc(alloc, data, pos);
        operands_initialized += 1;
    }

    const cast_type: ?RelationalRowsExpressionCastType = if (data[pos.*] == 1) blk: {
        pos.* += 1;
        const value: RelationalRowsExpressionCastType = @enumFromInt(data[pos.*]);
        pos.* += 1;
        break :blk value;
    } else blk: {
        pos.* += 1;
        break :blk null;
    };

    const branch_count = readU32(data, pos);
    const case_branches = try alloc.alloc(RelationalRowsExpressionCaseBranch, branch_count);
    var branches_initialized: usize = 0;
    errdefer {
        for (case_branches[0..branches_initialized]) |branch| freeRelationalRowsExpressionCaseBranch(alloc, branch);
        if (case_branches.len > 0) alloc.free(case_branches);
    }
    for (case_branches) |*branch| {
        const when = try readRelationalRowsExpressionConditionAlloc(alloc, data, pos);
        var when_transferred = false;
        errdefer if (!when_transferred) freeRelationalRowsExpressionCondition(alloc, when);
        const then = try readRelationalRowsExpressionAlloc(alloc, data, pos);
        var then_transferred = false;
        errdefer if (!then_transferred) freeRelationalRowsExpression(alloc, then);
        branch.* = .{ .when = when, .then = then };
        when_transferred = true;
        then_transferred = true;
        branches_initialized += 1;
    }

    const else_count = readU32(data, pos);
    const case_else = try alloc.alloc(RelationalRowsExpression, else_count);
    var else_initialized: usize = 0;
    errdefer {
        for (case_else[0..else_initialized]) |fallback| freeRelationalRowsExpression(alloc, fallback);
        if (case_else.len > 0) alloc.free(case_else);
    }
    for (case_else) |*fallback| {
        fallback.* = try readRelationalRowsExpressionAlloc(alloc, data, pos);
        else_initialized += 1;
    }

    return .{
        .kind = kind,
        .field = field,
        .field_source = field_source,
        .value_json = value_json,
        .json_path = json_path,
        .json_as_text = json_as_text,
        .operands = operands,
        .cast_type = cast_type,
        .case_branches = case_branches,
        .case_else = case_else,
    };
}

fn readU32(data: []const u8, pos: *usize) u32 {
    const val = std.mem.readInt(u32, data[pos.*..][0..4], .little);
    pos.* += 4;
    return val;
}

fn readU64(data: []const u8, pos: *usize) u64 {
    const val = std.mem.readInt(u64, data[pos.*..][0..8], .little);
    pos.* += 8;
    return val;
}

fn readStr(data: []const u8, pos: *usize) []const u8 {
    const len = readU32(data, pos);
    const s = data[pos.*..][0..len];
    pos.* += len;
    return s;
}

fn readStringSliceAlloc(alloc: Allocator, data: []const u8, pos: *usize) ![]const []const u8 {
    const count = readU32(data, pos);
    if (count == 0) return &.{};
    const out = try alloc.alloc([]const u8, count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| alloc.free(value);
        alloc.free(out);
    }
    for (out) |*value| {
        value.* = try alloc.dupe(u8, readStr(data, pos));
        initialized += 1;
    }
    return out;
}

fn readRelationalIndexOwnerRangeSliceAlloc(alloc: Allocator, data: []const u8, pos: *usize) ![]const RelationalIndexOwnerRange {
    const count = readU32(data, pos);
    if (count == 0) return &.{};
    const out = try alloc.alloc(RelationalIndexOwnerRange, count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |range| freeRelationalIndexOwnerRange(alloc, range);
        alloc.free(out);
    }
    for (out) |*range| {
        const start = try alloc.dupe(u8, readStr(data, pos));
        errdefer alloc.free(start);
        const end = try alloc.dupe(u8, readStr(data, pos));
        errdefer alloc.free(end);
        const range_id = try readOptStrAlloc(alloc, data, pos);
        errdefer if (range_id) |value| alloc.free(value);
        range.* = .{
            .start = start,
            .end = end,
            .range_id = range_id,
            .placement_generation = readU64(data, pos),
        };
        initialized += 1;
    }
    return out;
}

fn readRelationalIndexGenerationRecordAlloc(
    alloc: Allocator,
    data: []const u8,
    pos: *usize,
) !?RelationalIndexGenerationRecord {
    if (data[pos.*] == 0) {
        pos.* += 1;
        return null;
    }
    pos.* += 1;
    const generation = readU64(data, pos);
    const owner_ranges = try readRelationalIndexOwnerRangeSliceAlloc(alloc, data, pos);
    errdefer freeRelationalIndexOwnerRangeSlice(alloc, owner_ranges);
    const lifecycle: RelationalIndexLifecycle = @enumFromInt(data[pos.*]);
    pos.* += 1;
    const lag = readU64(data, pos);
    const failure_reason = try readOptStrAlloc(alloc, data, pos);
    errdefer if (failure_reason) |reason| alloc.free(reason);
    const ready_watermark = readU64(data, pos);
    return .{
        .generation = generation,
        .owner_ranges = owner_ranges,
        .lifecycle = lifecycle,
        .lag = lag,
        .failure_reason = failure_reason,
        .ready_watermark = ready_watermark,
    };
}

fn readRelationalIndexPlannerCapabilities(data: []const u8, pos: *usize) RelationalIndexPlannerCapabilities {
    const capabilities = RelationalIndexPlannerCapabilities{
        .equality = data[pos.*] == 1,
        .range = data[pos.* + 1] == 1,
        .ordering = data[pos.* + 2] == 1,
        .prefix = data[pos.* + 3] == 1,
        .full_text = data[pos.* + 4] == 1,
        .array = data[pos.* + 5] == 1,
        .json = data[pos.* + 6] == 1,
        .covering = data[pos.* + 7] == 1,
        .rank = data[pos.* + 8] == 1,
        .algebraic_dictionary = data[pos.* + 9] == 1,
        .algebraic_fact = data[pos.* + 10] == 1,
        .algebraic_path = data[pos.* + 11] == 1,
    };
    pos.* += 12;
    return capabilities;
}

fn readRelationalIndexKeySliceAlloc(alloc: Allocator, data: []const u8, pos: *usize) ![]const RelationalIndexKey {
    const count = readU32(data, pos);
    if (count == 0) return &.{};
    const out = try alloc.alloc(RelationalIndexKey, count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |key| {
            alloc.free(key.column);
            if (key.collation) |collation| alloc.free(collation);
        }
        alloc.free(out);
    }
    for (out) |*key| {
        const column = try alloc.dupe(u8, readStr(data, pos));
        errdefer alloc.free(column);
        const collation = try readOptStrAlloc(alloc, data, pos);
        errdefer if (collation) |value| alloc.free(value);
        const direction: RelationalIndexKeyDirection = @enumFromInt(data[pos.*]);
        pos.* += 1;
        const nulls: RelationalIndexKeyNulls = @enumFromInt(data[pos.*]);
        pos.* += 1;
        key.* = .{ .column = column, .collation = collation, .direction = direction, .nulls = nulls };
        initialized += 1;
    }
    return out;
}

fn readUniqueExpressionSliceAlloc(alloc: Allocator, data: []const u8, pos: *usize, fmt_version: u32) ![]const UniqueExpression {
    const count = readU32(data, pos);
    if (count == 0) return &.{};
    const out = try alloc.alloc(UniqueExpression, count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |expression| {
            alloc.free(expression.field);
            if (expression.expression) |row_expression| freeRelationalRowsExpression(alloc, row_expression);
        }
        alloc.free(out);
    }
    for (out) |*expression| {
        const op: UniqueExpressionOp = switch (data[pos.*]) {
            0 => .lower,
            1 => .upper,
            2 => .md5,
            3 => .expression,
            else => return error.InvalidSchema,
        };
        pos.* += 1;
        const field = try alloc.dupe(u8, readStr(data, pos));
        errdefer alloc.free(field);
        const row_expression: ?RelationalRowsExpression = if (fmt_version >= 41 and data[pos.*] == 1) expression_blk: {
            pos.* += 1;
            break :expression_blk try readRelationalRowsExpressionAlloc(alloc, data, pos);
        } else expression_blk: {
            if (fmt_version >= 41) pos.* += 1;
            break :expression_blk null;
        };
        errdefer if (row_expression) |value| freeRelationalRowsExpression(alloc, value);
        expression.* = .{
            .op = op,
            .field = field,
            .expression = row_expression,
        };
        initialized += 1;
    }
    return out;
}

fn readUniquePredicateSliceAlloc(alloc: Allocator, data: []const u8, pos: *usize) ![]const UniquePredicate {
    const count = readU32(data, pos);
    if (count == 0) return &.{};
    const out = try alloc.alloc(UniquePredicate, count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |predicate| {
            alloc.free(predicate.field);
            if (predicate.value_json) |value| alloc.free(value);
        }
        alloc.free(out);
    }
    for (out) |*predicate| {
        const op: UniquePredicateOp = switch (data[pos.*]) {
            0 => .is_null,
            1 => .is_not_null,
            2 => .eq,
            3 => .ne,
            else => return error.InvalidSchema,
        };
        pos.* += 1;
        const field = try alloc.dupe(u8, readStr(data, pos));
        errdefer alloc.free(field);
        const value_json = try readOptStrAlloc(alloc, data, pos);
        errdefer if (value_json) |value| alloc.free(value);
        predicate.* = .{
            .field = field,
            .op = op,
            .value_json = value_json,
        };
        initialized += 1;
    }
    return out;
}

// ============================================================================
// Tests
// ============================================================================

test "schema serialize/deserialize round-trip" {
    const alloc = std.testing.allocator;

    const schema = TableSchema{
        .version = 42,
        .default_type = "my_type",
        .ttl_duration_ns = 86400_000_000_000,
        .ttl_field = "_created",
        .enforce_types = true,
        .dynamic_templates = &.{
            .{
                .name = "dates",
                .match_pattern = "*_at",
                .unmatch_pattern = "skip_*",
                .path_match = "meta.*",
                .path_unmatch = "meta.private.*",
                .match_mapping_type = "date",
                .mapping = .{
                    .field_type = .datetime,
                    .do_index = false,
                    .store = false,
                    .doc_values = true,
                    .include_in_all = false,
                    .analyzer = "keyword",
                },
            },
        },
        .full_text_documents = &.{
            .{
                .name = "my_type",
                .fields = &.{
                    .{
                        .path = "title",
                        .emitted_name = "title",
                        .analyzer = "standard",
                        .include_in_all = true,
                    },
                    .{
                        .path = "title",
                        .emitted_name = "title._2gram",
                        .analyzer = "search_as_you_type_2gram",
                    },
                    .{
                        .path = "title",
                        .emitted_name = "title._3gram",
                        .analyzer = "search_as_you_type_3gram",
                    },
                    .{
                        .path = "title",
                        .emitted_name = "title._index_prefix",
                        .analyzer = "search_as_you_type_index_prefix",
                    },
                },
                .dynamic_rules = &.{
                    .{
                        .parent_path = "meta",
                        .segment_pattern = "^tag_[a-z]+$",
                        .relative_path = "title",
                        .variants = &.{
                            .{
                                .suffix = "",
                                .analyzer = "standard",
                            },
                            .{
                                .suffix = "._2gram",
                                .analyzer = "search_as_you_type_2gram",
                            },
                            .{
                                .suffix = "._3gram",
                                .analyzer = "search_as_you_type_3gram",
                            },
                            .{
                                .suffix = "._index_prefix",
                                .analyzer = "search_as_you_type_index_prefix",
                            },
                        },
                    },
                },
                .open_dynamic_paths = &.{ "", "meta" },
                .infer_type_dynamic_paths = &.{"typed"},
            },
        },
    };

    const data = try serializeSchema(alloc, schema);
    defer alloc.free(data);

    const loaded = try deserializeSchema(alloc, data);
    defer freeSchema(alloc, loaded);
    try std.testing.expectEqual(@as(u32, 42), loaded.version);
    try std.testing.expectEqualStrings("my_type", loaded.default_type);
    try std.testing.expectEqual(@as(u64, 86400_000_000_000), loaded.ttl_duration_ns);
    try std.testing.expectEqualStrings("_created", loaded.ttl_field);
    try std.testing.expect(loaded.enforce_types);
    try std.testing.expectEqual(@as(usize, 1), loaded.dynamic_templates.len);
    try std.testing.expectEqualStrings("dates", loaded.dynamic_templates[0].name);
    try std.testing.expectEqualStrings("skip_*", loaded.dynamic_templates[0].unmatch_pattern.?);
    try std.testing.expectEqualStrings("meta.private.*", loaded.dynamic_templates[0].path_unmatch.?);
    try std.testing.expectEqualStrings("date", loaded.dynamic_templates[0].match_mapping_type.?);
    try std.testing.expectEqual(AntflyType.datetime, loaded.dynamic_templates[0].mapping.field_type);
    try std.testing.expect(!loaded.dynamic_templates[0].mapping.do_index);
    try std.testing.expect(loaded.dynamic_templates[0].mapping.doc_values);
    try std.testing.expectEqual(@as(usize, 1), loaded.full_text_documents.len);
    try std.testing.expectEqualStrings("my_type", loaded.full_text_documents[0].name);
    try std.testing.expectEqual(@as(usize, 4), loaded.full_text_documents[0].fields.len);
    try std.testing.expectEqualStrings("title._2gram", loaded.full_text_documents[0].fields[1].emitted_name);
    try std.testing.expectEqualStrings("search_as_you_type_2gram", loaded.full_text_documents[0].fields[1].analyzer);
    try std.testing.expectEqualStrings("title._3gram", loaded.full_text_documents[0].fields[2].emitted_name);
    try std.testing.expectEqualStrings("search_as_you_type_3gram", loaded.full_text_documents[0].fields[2].analyzer);
    try std.testing.expectEqualStrings("title._index_prefix", loaded.full_text_documents[0].fields[3].emitted_name);
    try std.testing.expectEqualStrings("search_as_you_type_index_prefix", loaded.full_text_documents[0].fields[3].analyzer);
    try std.testing.expectEqual(@as(usize, 1), loaded.full_text_documents[0].dynamic_rules.len);
    try std.testing.expectEqualStrings("meta", loaded.full_text_documents[0].dynamic_rules[0].parent_path);
    try std.testing.expectEqualStrings("^tag_[a-z]+$", loaded.full_text_documents[0].dynamic_rules[0].segment_pattern.?);
    try std.testing.expectEqualStrings("title", loaded.full_text_documents[0].dynamic_rules[0].relative_path);
    try std.testing.expectEqual(@as(usize, 4), loaded.full_text_documents[0].dynamic_rules[0].variants.len);
    try std.testing.expectEqualStrings("._2gram", loaded.full_text_documents[0].dynamic_rules[0].variants[1].suffix);
    try std.testing.expectEqualStrings("._3gram", loaded.full_text_documents[0].dynamic_rules[0].variants[2].suffix);
    try std.testing.expectEqualStrings("._index_prefix", loaded.full_text_documents[0].dynamic_rules[0].variants[3].suffix);
    try std.testing.expectEqual(@as(usize, 2), loaded.full_text_documents[0].open_dynamic_paths.len);
    try std.testing.expectEqualStrings("", loaded.full_text_documents[0].open_dynamic_paths[0]);
    try std.testing.expectEqualStrings("meta", loaded.full_text_documents[0].open_dynamic_paths[1]);
    try std.testing.expectEqual(@as(usize, 1), loaded.full_text_documents[0].infer_type_dynamic_paths.len);
    try std.testing.expectEqualStrings("typed", loaded.full_text_documents[0].infer_type_dynamic_paths[0]);

    // Default document mode round-trips with no relational columns.
    try std.testing.expectEqual(StorageMode.document, loaded.storage_mode);
    try std.testing.expectEqual(@as(usize, 0), loaded.relational_columns.len);
    try std.testing.expectEqual(@as(usize, 0), loaded.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 0), loaded.unique_constraints.len);
    try std.testing.expect(!loaded.system_versioned);
}

test "schema serialize/deserialize round-trips relational storage mode and columns" {
    const alloc = std.testing.allocator;

    const schema = TableSchema{
        .version = 7,
        .default_type = "row",
        .enforce_types = true,
        .storage_mode = .relational,
        .relational_columns = &.{
            .{ .name = "id", .path = "id", .field_type = .keyword, .collation = "C", .nullable = false },
            .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword, .nullable = false, .cardinality_proof = .unique },
            .{
                .name = "amount",
                .path = "amount",
                .field_type = .numeric,
                .nullable = false,
                .default_value = .{ .value_json = "1" },
            },
            .{ .name = "created_at", .path = "created_at", .field_type = .datetime, .nullable = true, .default_value = .{ .kind = .now_ns, .value_json = "" }, .on_update_value = .{ .kind = .now_ns, .value_json = "" } },
            .{ .name = "request_id", .path = "request_id", .field_type = .keyword, .nullable = true, .default_value = .{ .kind = .uuid_v4, .value_json = "" } },
            .{ .name = "created_day", .path = "created_day", .field_type = .datetime, .nullable = true, .default_value = .{ .kind = .current_date_ns, .value_json = "" } },
            .{ .name = "sequence_id", .path = "sequence_id", .field_type = .numeric, .nullable = false, .default_value = .{ .kind = .sequence_next, .value_json = "{\"sequence\":\"orders_id_seq\",\"database\":\"tenant\",\"schema\":\"billing\"}" } },
            .{ .name = "payload", .path = "payload", .field_type = .json, .nullable = true },
            .{ .name = "tags", .path = "tags", .field_type = .array, .array_item_type = .keyword, .nullable = true },
            .{ .name = "tenant_key", .path = "tenant_key", .field_type = .keyword, .nullable = true, .generated = .{ .op = .lower, .field = "tenant_id" } },
            .{ .name = "tenant_upper_key", .path = "tenant_upper_key", .field_type = .keyword, .nullable = true, .generated = .{ .op = .upper, .field = "tenant_id" } },
            .{ .name = "default_status", .path = "default_status", .field_type = .keyword, .nullable = true, .default_value = .{ .kind = .scalar_subquery, .value_json = "{\"query\":{\"table\":\"orders\",\"select\":[\"status\"],\"limit\":1}}" } },
        },
        .primary_key = .{
            .name = "orders_pkey",
            .columns = &.{ "tenant_id", "id" },
            .include_columns = &.{ "created_at", "request_id" },
            .without_overlaps_period = "valid_time",
            .deferrable = true,
            .timing = .deferred,
        },
        .periods = &.{.{ .name = "valid_time", .start_column = "created_at", .end_column = "created_day", .range_type = .daterange }},
        .foreign_keys = &.{
            .{
                .name = "orders_customer_id_fkey",
                .child_columns = &.{"customer_id"},
                .parent_table = "customers",
                .parent_columns = &.{"_id"},
            },
            .{
                .name = "orders_referrer_id_fkey",
                .child_columns = &.{"referrer_id"},
                .parent_table = "customers",
                .parent_columns = &.{"_id"},
                .on_delete = .set_null,
            },
            .{
                .name = "orders_account_id_fkey",
                .child_columns = &.{"account_id"},
                .child_period = "valid_time",
                .parent_table = "accounts",
                .parent_columns = &.{"_id"},
                .parent_period = "valid_time",
                .on_delete = .cascade,
                .on_update = .no_action,
                .timing = .immediate,
                .deferrable = true,
                .match = .simple,
                .validation_state = .enforced,
            },
        },
        .unique_constraints = &.{
            .{
                .name = "users_tenant_email_key",
                .columns = &.{ "tenant_id", "email" },
                .include_columns = &.{ "created_at", "request_id" },
                .nulls_not_distinct = true,
                .deferrable = true,
                .timing = .deferred,
                .where = &.{
                    .{ .field = "email", .op = .is_not_null },
                },
                .where_expressions = &.{.{
                    .lhs = .{ .kind = .lower, .operands = &.{.{ .kind = .field, .field = "email" }} },
                    .op = .is_not_null,
                }},
            },
            .{
                .name = "users_lower_email_key",
                .columns = &.{"tenant_id"},
                .expressions = &.{
                    .{ .op = .lower, .field = "email" },
                },
                .validation_state = .unvalidated,
            },
            .{
                .name = "users_upper_email_key",
                .columns = &.{"tenant_id"},
                .expressions = &.{
                    .{ .op = .upper, .field = "email" },
                },
            },
            .{
                .name = "users_md5_email_key",
                .columns = &.{"tenant_id"},
                .without_overlaps_period = "valid_time",
                .expressions = &.{
                    .{ .op = .md5, .field = "email" },
                },
            },
        },
        .relational_indexes = &.{
            .{
                .name = "amount_cover_idx",
                .owner_kind = .relational_column,
                .owner_name = "amount",
                .access_method = .ordered_tuple,
                .columns = &.{"amount"},
                .include_columns = &.{ "tenant_id", "created_at" },
                .keys = &.{
                    .{ .column = "amount", .direction = .desc, .nulls = .last },
                    .{ .column = "tenant_id", .direction = .asc, .nulls = .first },
                },
                .lifecycle = .building,
                .generation = 12345,
                .schema_fingerprint = "secondary-index-v1:test",
                .owner_ranges = &.{
                    .{
                        .start = "row:a",
                        .end = "row:m",
                        .range_id = "range-1",
                        .placement_generation = 77,
                    },
                    .{
                        .start = "row:m",
                        .end = "",
                        .range_id = "range-2",
                        .placement_generation = 78,
                    },
                },
                .generation_record = .{
                    .generation = 12345,
                    .owner_ranges = &.{
                        .{
                            .start = "row:a",
                            .end = "row:m",
                            .range_id = "range-1",
                            .placement_generation = 77,
                        },
                        .{
                            .start = "row:m",
                            .end = "",
                            .range_id = "range-2",
                            .placement_generation = 78,
                        },
                    },
                    .lifecycle = .building,
                    .lag = 12,
                    .failure_reason = "catch-up lag",
                    .ready_watermark = 9876,
                },
                .planner_capabilities = .{
                    .equality = true,
                    .range = true,
                    .ordering = true,
                    .covering = true,
                },
                .where = &.{.{ .field = "tenant_id", .op = .is_not_null }},
                .where_expressions = &.{.{
                    .lhs = .{ .kind = .lower, .operands = &.{.{ .kind = .field, .field = "tenant_id" }} },
                    .op = .eq,
                    .rhs = &.{.{ .kind = .value, .value_json = "\"t1\"" }},
                }},
            },
            .{
                .name = "users_tenant_email_key",
                .owner_kind = .unique_constraint,
                .owner_name = "users_tenant_email_key",
                .access_method = .ordered_tuple,
                .unique = true,
                .columns = &.{ "tenant_id", "email" },
                .include_columns = &.{ "created_at", "request_id" },
                .keys = &.{
                    .{ .column = "tenant_id", .direction = .asc, .nulls = .default },
                    .{ .column = "email", .direction = .desc, .nulls = .last },
                },
                .lifecycle = .building,
                .generation = 67890,
                .schema_fingerprint = "secondary-index-v1:users_tenant_email_key",
                .where = &.{
                    .{ .field = "email", .op = .is_not_null },
                },
                .where_expressions = &.{.{
                    .lhs = .{ .kind = .lower, .operands = &.{.{ .kind = .field, .field = "email" }} },
                    .op = .is_not_null,
                }},
            },
            .{
                .name = "row_algebraic_idx",
                .owner_kind = .table,
                .owner_name = relational_table_index_owner_name,
                .access_method = .algebraic_filter,
                .method_config_json = "{\"type\":\"algebraic\",\"derive_from_schema\":true}",
                .lifecycle = .building,
                .generation = 13579,
                .schema_fingerprint = "secondary-index-v1:algebraic",
            },
        },
        .checks = &.{
            .{ .name = "tenant_case_match", .field = "tenant_id", .op = .eq, .value_json = "\"TENANT\"", .collation = "antfly.case_insensitive" },
            .{ .name = "amount_nonnegative", .field = "amount", .op = .gte, .value_json = "0" },
            .{
                .name = "amount_plus_fee_positive",
                .expression = .{
                    .lhs = .{
                        .kind = .add,
                        .operands = &.{
                            .{ .kind = .field, .field = "amount" },
                            .{ .kind = .field, .field = "fee" },
                        },
                    },
                    .op = .gt,
                    .rhs = &.{.{ .kind = .value, .value_json = "0" }},
                },
            },
        },
        .external_base_source = .{
            .table_id = "orders",
            .format = .iceberg,
            .source_uri = "s3://bucket/warehouse/orders",
            .credential_ref = .{ .ref_id = "prod-lake-read", .scope = "orders" },
            .snapshot_mode = .{ .snapshot_id = "iceberg-123" },
            .schema_fingerprint = "schema-v7",
        },
        .system_versioned = true,
    };

    const data = try serializeSchema(alloc, schema);
    defer alloc.free(data);
    try std.testing.expectEqual(@as(u32, 56), std.mem.readInt(u32, data[4..8], .little));

    var downgraded = try alloc.dupe(u8, data);
    defer alloc.free(downgraded);
    std.mem.writeInt(u32, downgraded[4..8], 51, .little);
    try std.testing.expectError(error.UnsupportedVersion, deserializeSchema(alloc, downgraded));

    const loaded = try deserializeSchema(alloc, data);
    defer freeSchema(alloc, loaded);

    try std.testing.expectEqual(StorageMode.relational, loaded.storage_mode);
    try std.testing.expectEqual(@as(usize, 12), loaded.relational_columns.len);
    try std.testing.expectEqualStrings("id", loaded.relational_columns[0].name);
    try std.testing.expectEqualStrings("id", loaded.relational_columns[0].path);
    try std.testing.expectEqual(AntflyType.keyword, loaded.relational_columns[0].field_type);
    try std.testing.expectEqualStrings("C", loaded.relational_columns[0].collation.?);
    try std.testing.expect(!loaded.relational_columns[0].nullable);
    try std.testing.expectEqual(AntflyType.keyword, loaded.relational_columns[1].field_type);
    try std.testing.expect(!loaded.relational_columns[1].nullable);
    try std.testing.expectEqual(RelationalColumnCardinalityProof.unique, loaded.relational_columns[1].cardinality_proof);
    try std.testing.expect(loaded.primary_key != null);
    try std.testing.expectEqualStrings("orders_pkey", loaded.primary_key.?.name.?);
    try std.testing.expectEqual(@as(usize, 2), loaded.primary_key.?.columns.len);
    try std.testing.expectEqualStrings("tenant_id", loaded.primary_key.?.columns[0]);
    try std.testing.expectEqualStrings("id", loaded.primary_key.?.columns[1]);
    try std.testing.expectEqual(@as(usize, 2), loaded.primary_key.?.include_columns.len);
    try std.testing.expectEqualStrings("created_at", loaded.primary_key.?.include_columns[0]);
    try std.testing.expectEqualStrings("request_id", loaded.primary_key.?.include_columns[1]);
    try std.testing.expectEqualStrings("valid_time", loaded.primary_key.?.without_overlaps_period.?);
    try std.testing.expect(loaded.primary_key.?.deferrable);
    try std.testing.expectEqual(ForeignKeyTiming.deferred, loaded.primary_key.?.timing);
    try std.testing.expectEqual(@as(usize, 1), loaded.periods.len);
    try std.testing.expectEqualStrings("valid_time", loaded.periods[0].name);
    try std.testing.expectEqualStrings("created_at", loaded.periods[0].start_column);
    try std.testing.expectEqualStrings("created_day", loaded.periods[0].end_column);
    try std.testing.expectEqual(RelationalPeriodRangeType.daterange, loaded.periods[0].range_type.?);
    try std.testing.expectEqual(AntflyType.numeric, loaded.relational_columns[2].field_type);
    try std.testing.expectEqual(AntflyType.datetime, loaded.relational_columns[3].field_type);
    try std.testing.expect(loaded.relational_columns[3].nullable);
    try std.testing.expect(loaded.relational_columns[3].default_value != null);
    try std.testing.expectEqual(RelationalDefaultKind.now_ns, loaded.relational_columns[3].default_value.?.kind);
    try std.testing.expect(loaded.relational_columns[3].on_update_value != null);
    try std.testing.expectEqual(RelationalDefaultKind.now_ns, loaded.relational_columns[3].on_update_value.?.kind);
    try std.testing.expectEqual(AntflyType.keyword, loaded.relational_columns[4].field_type);
    try std.testing.expect(loaded.relational_columns[4].default_value != null);
    try std.testing.expectEqual(RelationalDefaultKind.uuid_v4, loaded.relational_columns[4].default_value.?.kind);
    try std.testing.expectEqual(AntflyType.datetime, loaded.relational_columns[5].field_type);
    try std.testing.expect(loaded.relational_columns[5].default_value != null);
    try std.testing.expectEqual(RelationalDefaultKind.current_date_ns, loaded.relational_columns[5].default_value.?.kind);
    try std.testing.expectEqual(AntflyType.numeric, loaded.relational_columns[6].field_type);
    try std.testing.expect(loaded.relational_columns[6].default_value != null);
    try std.testing.expectEqual(RelationalDefaultKind.sequence_next, loaded.relational_columns[6].default_value.?.kind);
    try std.testing.expectEqualStrings("{\"sequence\":\"orders_id_seq\",\"database\":\"tenant\",\"schema\":\"billing\"}", loaded.relational_columns[6].default_value.?.value_json);
    try std.testing.expectEqual(AntflyType.keyword, loaded.relational_columns[11].field_type);
    try std.testing.expect(loaded.relational_columns[11].default_value != null);
    try std.testing.expectEqual(RelationalDefaultKind.scalar_subquery, loaded.relational_columns[11].default_value.?.kind);
    try std.testing.expectEqualStrings("{\"query\":{\"table\":\"orders\",\"select\":[\"status\"],\"limit\":1}}", loaded.relational_columns[11].default_value.?.value_json);
    try std.testing.expectEqual(AntflyType.json, loaded.relational_columns[7].field_type);
    try std.testing.expectEqual(AntflyType.array, loaded.relational_columns[8].field_type);
    try std.testing.expectEqual(AntflyType.keyword, loaded.relational_columns[8].array_item_type.?);
    try std.testing.expect(loaded.relational_columns[2].default_value != null);
    try std.testing.expectEqualStrings("1", loaded.relational_columns[2].default_value.?.value_json);
    try std.testing.expect(loaded.relational_columns[9].generated != null);
    try std.testing.expectEqual(RelationalGeneratedOp.lower, loaded.relational_columns[9].generated.?.op);
    try std.testing.expectEqualStrings("tenant_id", loaded.relational_columns[9].generated.?.field.?);
    try std.testing.expect(loaded.relational_columns[10].generated != null);
    try std.testing.expectEqual(RelationalGeneratedOp.upper, loaded.relational_columns[10].generated.?.op);
    try std.testing.expectEqualStrings("tenant_id", loaded.relational_columns[10].generated.?.field.?);
    try std.testing.expectEqual(@as(usize, 3), loaded.foreign_keys.len);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", loaded.foreign_keys[0].name);
    try std.testing.expectEqualStrings("customer_id", loaded.foreign_keys[0].child_columns[0]);
    try std.testing.expectEqualStrings("customers", loaded.foreign_keys[0].parent_table);
    try std.testing.expectEqualStrings("_id", loaded.foreign_keys[0].parent_columns[0]);
    try std.testing.expectEqual(ForeignKeyAction.restrict, loaded.foreign_keys[0].on_delete);
    try std.testing.expectEqualStrings("orders_referrer_id_fkey", loaded.foreign_keys[1].name);
    try std.testing.expectEqual(ForeignKeyAction.set_null, loaded.foreign_keys[1].on_delete);
    try std.testing.expectEqualStrings("orders_account_id_fkey", loaded.foreign_keys[2].name);
    try std.testing.expectEqualStrings("valid_time", loaded.foreign_keys[2].child_period.?);
    try std.testing.expectEqualStrings("valid_time", loaded.foreign_keys[2].parent_period.?);
    try std.testing.expectEqual(ForeignKeyAction.cascade, loaded.foreign_keys[2].on_delete);
    try std.testing.expectEqual(ForeignKeyAction.no_action, loaded.foreign_keys[2].on_update);
    try std.testing.expectEqual(ForeignKeyTiming.immediate, loaded.foreign_keys[2].timing);
    try std.testing.expect(loaded.foreign_keys[2].deferrable);
    try std.testing.expectEqual(ForeignKeyMatch.simple, loaded.foreign_keys[2].match);
    try std.testing.expectEqual(ForeignKeyValidationState.enforced, loaded.foreign_keys[2].validation_state);
    try std.testing.expectEqual(@as(usize, 4), loaded.unique_constraints.len);
    try std.testing.expectEqualStrings("users_tenant_email_key", loaded.unique_constraints[0].name);
    try std.testing.expectEqual(@as(usize, 2), loaded.unique_constraints[0].columns.len);
    try std.testing.expectEqualStrings("tenant_id", loaded.unique_constraints[0].columns[0]);
    try std.testing.expectEqualStrings("email", loaded.unique_constraints[0].columns[1]);
    try std.testing.expectEqual(@as(usize, 2), loaded.unique_constraints[0].include_columns.len);
    try std.testing.expectEqualStrings("created_at", loaded.unique_constraints[0].include_columns[0]);
    try std.testing.expectEqualStrings("request_id", loaded.unique_constraints[0].include_columns[1]);
    try std.testing.expect(loaded.unique_constraints[0].nulls_not_distinct);
    try std.testing.expect(loaded.unique_constraints[0].deferrable);
    try std.testing.expectEqual(ForeignKeyTiming.deferred, loaded.unique_constraints[0].timing);
    try std.testing.expectEqual(@as(usize, 1), loaded.unique_constraints[0].where.len);
    try std.testing.expectEqualStrings("email", loaded.unique_constraints[0].where[0].field);
    try std.testing.expectEqual(UniquePredicateOp.is_not_null, loaded.unique_constraints[0].where[0].op);
    try std.testing.expectEqual(@as(usize, 1), loaded.unique_constraints[0].where_expressions.len);
    try std.testing.expectEqual(RelationalRowsExpressionKind.lower, loaded.unique_constraints[0].where_expressions[0].lhs.kind);
    try std.testing.expectEqualStrings("email", loaded.unique_constraints[0].where_expressions[0].lhs.operands[0].field);
    try std.testing.expectEqual(UniqueConstraintValidationState.enforced, loaded.unique_constraints[0].validation_state);
    try std.testing.expectEqualStrings("users_lower_email_key", loaded.unique_constraints[1].name);
    try std.testing.expectEqual(UniqueConstraintValidationState.unvalidated, loaded.unique_constraints[1].validation_state);
    try std.testing.expectEqual(@as(usize, 1), loaded.unique_constraints[1].expressions.len);
    try std.testing.expectEqual(UniqueExpressionOp.lower, loaded.unique_constraints[1].expressions[0].op);
    try std.testing.expectEqualStrings("email", loaded.unique_constraints[1].expressions[0].field);
    try std.testing.expectEqualStrings("users_upper_email_key", loaded.unique_constraints[2].name);
    try std.testing.expectEqual(@as(usize, 1), loaded.unique_constraints[2].expressions.len);
    try std.testing.expectEqual(UniqueExpressionOp.upper, loaded.unique_constraints[2].expressions[0].op);
    try std.testing.expectEqualStrings("email", loaded.unique_constraints[2].expressions[0].field);
    try std.testing.expectEqualStrings("users_md5_email_key", loaded.unique_constraints[3].name);
    try std.testing.expectEqualStrings("valid_time", loaded.unique_constraints[3].without_overlaps_period.?);
    try std.testing.expectEqual(@as(usize, 1), loaded.unique_constraints[3].expressions.len);
    try std.testing.expectEqual(UniqueExpressionOp.md5, loaded.unique_constraints[3].expressions[0].op);
    try std.testing.expectEqualStrings("email", loaded.unique_constraints[3].expressions[0].field);
    try std.testing.expectEqual(@as(usize, 3), loaded.relational_indexes.len);
    try std.testing.expectEqualStrings("amount_cover_idx", loaded.relational_indexes[0].name);
    try std.testing.expectEqual(RelationalIndexOwnerKind.relational_column, loaded.relational_indexes[0].owner_kind);
    try std.testing.expectEqualStrings("amount", loaded.relational_indexes[0].owner_name);
    try std.testing.expectEqual(RelationalIndexAccessMethod.ordered_tuple, loaded.relational_indexes[0].access_method);
    try std.testing.expect(!loaded.relational_indexes[0].unique);
    try std.testing.expectEqual(@as(usize, 1), loaded.relational_indexes[0].columns.len);
    try std.testing.expectEqualStrings("amount", loaded.relational_indexes[0].columns[0]);
    try std.testing.expectEqual(@as(usize, 2), loaded.relational_indexes[0].include_columns.len);
    try std.testing.expectEqualStrings("tenant_id", loaded.relational_indexes[0].include_columns[0]);
    try std.testing.expectEqual(@as(usize, 2), loaded.relational_indexes[0].keys.len);
    try std.testing.expectEqualStrings("amount", loaded.relational_indexes[0].keys[0].column);
    try std.testing.expectEqual(RelationalIndexLifecycle.building, loaded.relational_indexes[0].lifecycle);
    try std.testing.expectEqual(@as(u64, 12345), loaded.relational_indexes[0].generation);
    try std.testing.expectEqualStrings("secondary-index-v1:test", loaded.relational_indexes[0].schema_fingerprint.?);
    try std.testing.expectEqual(@as(usize, 2), loaded.relational_indexes[0].owner_ranges.len);
    try std.testing.expectEqualStrings("row:a", loaded.relational_indexes[0].owner_ranges[0].start);
    try std.testing.expectEqualStrings("row:m", loaded.relational_indexes[0].owner_ranges[0].end);
    try std.testing.expectEqualStrings("range-1", loaded.relational_indexes[0].owner_ranges[0].range_id.?);
    try std.testing.expectEqual(@as(u64, 77), loaded.relational_indexes[0].owner_ranges[0].placement_generation);
    try std.testing.expectEqualStrings("row:m", loaded.relational_indexes[0].owner_ranges[1].start);
    try std.testing.expectEqualStrings("", loaded.relational_indexes[0].owner_ranges[1].end);
    try std.testing.expectEqualStrings("range-2", loaded.relational_indexes[0].owner_ranges[1].range_id.?);
    const generation_record = loaded.relational_indexes[0].generation_record orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 12345), generation_record.generation);
    try std.testing.expectEqual(RelationalIndexLifecycle.building, generation_record.lifecycle);
    try std.testing.expectEqual(@as(u64, 12), generation_record.lag);
    try std.testing.expectEqualStrings("catch-up lag", generation_record.failure_reason.?);
    try std.testing.expectEqual(@as(u64, 9876), generation_record.ready_watermark);
    try std.testing.expectEqual(@as(usize, 2), generation_record.owner_ranges.len);
    try std.testing.expectEqualStrings("row:a", generation_record.owner_ranges[0].start);
    try std.testing.expectEqualStrings("row:m", generation_record.owner_ranges[0].end);
    try std.testing.expectEqualStrings("range-1", generation_record.owner_ranges[0].range_id.?);
    try std.testing.expect(loaded.relational_indexes[0].planner_capabilities.equality);
    try std.testing.expect(loaded.relational_indexes[0].planner_capabilities.range);
    try std.testing.expect(loaded.relational_indexes[0].planner_capabilities.ordering);
    try std.testing.expect(loaded.relational_indexes[0].planner_capabilities.covering);
    try std.testing.expect(!loaded.relational_indexes[0].planner_capabilities.full_text);
    try std.testing.expectEqual(@as(usize, 1), loaded.relational_indexes[0].where.len);
    try std.testing.expectEqualStrings("tenant_id", loaded.relational_indexes[0].where[0].field);
    try std.testing.expectEqual(@as(usize, 1), loaded.relational_indexes[0].where_expressions.len);
    try std.testing.expectEqualStrings("users_tenant_email_key", loaded.relational_indexes[1].name);
    try std.testing.expectEqual(RelationalIndexOwnerKind.unique_constraint, loaded.relational_indexes[1].owner_kind);
    try std.testing.expectEqualStrings("users_tenant_email_key", loaded.relational_indexes[1].owner_name);
    try std.testing.expect(loaded.relational_indexes[1].unique);
    try std.testing.expectEqual(@as(usize, 2), loaded.relational_indexes[1].columns.len);
    try std.testing.expectEqualStrings("tenant_id", loaded.relational_indexes[1].columns[0]);
    try std.testing.expectEqualStrings("email", loaded.relational_indexes[1].columns[1]);
    try std.testing.expectEqual(@as(u64, 67890), loaded.relational_indexes[1].generation);
    try std.testing.expectEqualStrings("row_algebraic_idx", loaded.relational_indexes[2].name);
    try std.testing.expectEqual(RelationalIndexOwnerKind.table, loaded.relational_indexes[2].owner_kind);
    try std.testing.expectEqualStrings(relational_table_index_owner_name, loaded.relational_indexes[2].owner_name);
    try std.testing.expectEqual(RelationalIndexAccessMethod.algebraic_filter, loaded.relational_indexes[2].access_method);
    try std.testing.expectEqualStrings("{\"type\":\"algebraic\",\"derive_from_schema\":true}", loaded.relational_indexes[2].method_config_json.?);
    try std.testing.expectEqual(@as(usize, 3), loaded.checks.len);
    try std.testing.expectEqualStrings("tenant_case_match", loaded.checks[0].name);
    try std.testing.expectEqual(RelationalCheckOp.eq, loaded.checks[0].op);
    try std.testing.expectEqualStrings("\"TENANT\"", loaded.checks[0].value_json.?);
    try std.testing.expectEqualStrings("antfly.case_insensitive", loaded.checks[0].collation.?);
    try std.testing.expectEqualStrings("amount_nonnegative", loaded.checks[1].name);
    try std.testing.expectEqual(RelationalCheckOp.gte, loaded.checks[1].op);
    try std.testing.expectEqualStrings("0", loaded.checks[1].value_json.?);
    try std.testing.expect(loaded.checks[1].collation == null);
    try std.testing.expectEqualStrings("amount_plus_fee_positive", loaded.checks[2].name);
    try std.testing.expect(loaded.checks[2].expression != null);
    try std.testing.expectEqual(RelationalRowsExpressionKind.add, loaded.checks[2].expression.?.lhs.kind);
    try std.testing.expectEqual(@as(usize, 2), loaded.checks[2].expression.?.lhs.operands.len);
    try std.testing.expectEqualStrings("amount", loaded.checks[2].expression.?.lhs.operands[0].field);
    try std.testing.expectEqual(RelationalCheckOp.gt, loaded.checks[2].expression.?.op);
    try std.testing.expectEqualStrings("0", loaded.checks[2].expression.?.rhs[0].value_json);
    try std.testing.expect(loaded.external_base_source != null);
    try std.testing.expectEqualStrings("orders", loaded.external_base_source.?.table_id);
    try std.testing.expectEqual(ExternalBaseFormat.iceberg, loaded.external_base_source.?.format);
    try std.testing.expectEqualStrings("s3://bucket/warehouse/orders", loaded.external_base_source.?.source_uri);
    try std.testing.expect(loaded.external_base_source.?.credential_ref != null);
    try std.testing.expectEqualStrings("prod-lake-read", loaded.external_base_source.?.credential_ref.?.ref_id);
    try std.testing.expectEqualStrings("orders", loaded.external_base_source.?.credential_ref.?.scope);
    try std.testing.expectEqualStrings("iceberg-123", loaded.external_base_source.?.snapshot_mode.snapshot_id);
    try std.testing.expectEqualStrings("schema-v7", loaded.external_base_source.?.schema_fingerprint);
    try std.testing.expectEqual(ExternalWritePolicy.read_only, loaded.external_base_source.?.write_policy);
    try std.testing.expect(loaded.system_versioned);
}

test "schema relational check clone preserves expression AST" {
    const alloc = std.testing.allocator;

    const operands = [_]RelationalRowsExpression{
        .{ .kind = .field, .field = "amount" },
        .{ .kind = .field, .field = "fee" },
    };
    const rhs = [_]RelationalRowsExpression{.{ .kind = .value, .value_json = "0" }};
    const check = RelationalCheck{
        .name = "amount_plus_fee_positive",
        .collation = "antfly.case_insensitive",
        .expression = .{
            .lhs = .{ .kind = .add, .operands = operands[0..] },
            .op = .gt,
            .rhs = rhs[0..],
        },
    };

    const cloned = try cloneRelationalCheckAlloc(alloc, check);
    defer freeRelationalCheck(alloc, cloned);

    try std.testing.expect(relationalChecksEqual(check, cloned));
    try std.testing.expectEqualStrings("antfly.case_insensitive", cloned.collation.?);
    try std.testing.expect(cloned.collation.?.ptr != check.collation.?.ptr);
    try std.testing.expect(cloned.expression != null);
    try std.testing.expectEqual(RelationalRowsExpressionKind.add, cloned.expression.?.lhs.kind);
    try std.testing.expectEqual(@as(usize, 2), cloned.expression.?.lhs.operands.len);
    try std.testing.expectEqualStrings("amount", cloned.expression.?.lhs.operands[0].field);
    try std.testing.expect(cloned.expression.?.lhs.operands[0].field.ptr != operands[0].field.ptr);
    try std.testing.expectEqualStrings("0", cloned.expression.?.rhs[0].value_json);
    try std.testing.expect(cloned.expression.?.rhs[0].value_json.ptr != rhs[0].value_json.ptr);
}

test "schema save/load via DocStore" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "schema-store");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    // No schema initially
    const none = try loadSchema(&store, alloc);
    try std.testing.expect(none == null);

    // Save and reload
    const schema = TableSchema{ .version = 7, .default_type = "doc" };
    try saveSchema(&store, alloc, schema);

    const loaded = (try loadSchema(&store, alloc)).?;
    defer freeSchema(alloc, loaded);
    try std.testing.expectEqual(@as(u32, 7), loaded.version);
    try std.testing.expectEqualStrings("doc", loaded.default_type);

    const loaded_v7 = (try loadSchemaVersion(&store, alloc, 7)).?;
    defer freeSchema(alloc, loaded_v7);
    try std.testing.expectEqual(@as(u32, 7), loaded_v7.version);
}

test "schema preserves versioned history in DocStore" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "schema-history");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    try saveSchema(&store, alloc, .{ .version = 0, .default_type = "doc_v0" });
    try saveSchema(&store, alloc, .{ .version = 1, .default_type = "doc_v1" });

    const active = (try loadSchema(&store, alloc)).?;
    defer freeSchema(alloc, active);
    try std.testing.expectEqual(@as(u32, 1), active.version);
    try std.testing.expectEqualStrings("doc_v1", active.default_type);

    const previous = (try loadSchemaVersion(&store, alloc, 0)).?;
    defer freeSchema(alloc, previous);
    try std.testing.expectEqual(@as(u32, 0), previous.version);
    try std.testing.expectEqualStrings("doc_v0", previous.default_type);
}

test "schema copy includes versioned history" {
    const alloc = std.testing.allocator;
    const src_path = try tempTestPath(alloc, "schema-copy-src");
    defer alloc.free(src_path);
    cleanupTestDir(src_path);
    defer cleanupTestDir(src_path);

    const dst_path = try tempTestPath(alloc, "schema-copy-dst");
    defer alloc.free(dst_path);
    cleanupTestDir(dst_path);
    defer cleanupTestDir(dst_path);

    var src = try DocStore.open(alloc, src_path, .{});
    defer src.close();
    var dst = try DocStore.open(alloc, dst_path, .{});
    defer dst.close();

    try saveSchema(&src, alloc, .{ .version = 0, .default_type = "doc_v0" });
    try saveSchema(&src, alloc, .{ .version = 1, .default_type = "doc_v1" });
    try copySchemas(&src, &dst, alloc);

    const active = (try loadSchema(&dst, alloc)).?;
    defer freeSchema(alloc, active);
    try std.testing.expectEqual(@as(u32, 1), active.version);
    try std.testing.expectEqualStrings("doc_v1", active.default_type);

    const previous = (try loadSchemaVersion(&dst, alloc, 0)).?;
    defer freeSchema(alloc, previous);
    try std.testing.expectEqual(@as(u32, 0), previous.version);
    try std.testing.expectEqualStrings("doc_v0", previous.default_type);
}

test "schema save upgrades legacy active-only schema into versioned history" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "schema-legacy-upgrade");
    defer alloc.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);

    var store = try DocStore.open(alloc, path, .{});
    defer store.close();

    const legacy_data = try serializeSchema(alloc, .{ .version = 0, .default_type = "legacy_v0" });
    defer alloc.free(legacy_data);
    try store.put(schema_key, legacy_data);

    try saveSchema(&store, alloc, .{ .version = 1, .default_type = "next_v1" });

    const active = (try loadSchema(&store, alloc)).?;
    defer freeSchema(alloc, active);
    try std.testing.expectEqual(@as(u32, 1), active.version);
    try std.testing.expectEqualStrings("next_v1", active.default_type);

    const previous = (try loadSchemaVersion(&store, alloc, 0)).?;
    defer freeSchema(alloc, previous);
    try std.testing.expectEqual(@as(u32, 0), previous.version);
    try std.testing.expectEqualStrings("legacy_v0", previous.default_type);
}

test "schema save/load via memory backend store" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    const none = try loadSchema(runtime, alloc);
    try std.testing.expect(none == null);

    const schema = TableSchema{ .version = 11, .default_type = "memdoc" };
    try saveSchema(runtime, alloc, schema);

    const loaded = (try loadSchema(runtime, alloc)).?;
    defer freeSchema(alloc, loaded);
    try std.testing.expectEqual(@as(u32, 11), loaded.version);
    try std.testing.expectEqualStrings("memdoc", loaded.default_type);
}

test "schema save/load via lsm backend store" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    const none = try loadSchema(runtime, alloc);
    try std.testing.expect(none == null);

    const schema = TableSchema{ .version = 12, .default_type = "lsmdoc" };
    try saveSchema(runtime, alloc, schema);

    const loaded = (try loadSchema(runtime, alloc)).?;
    defer freeSchema(alloc, loaded);
    try std.testing.expectEqual(@as(u32, 12), loaded.version);
    try std.testing.expectEqualStrings("lsmdoc", loaded.default_type);
}

test "glob matching" {
    // Exact
    try std.testing.expect(globMatch("hello", "hello"));
    try std.testing.expect(!globMatch("hello", "world"));

    // Wildcard *
    try std.testing.expect(globMatch("*_embedding", "title_embedding"));
    try std.testing.expect(globMatch("*_embedding", "desc_embedding"));
    try std.testing.expect(!globMatch("*_embedding", "title_text"));

    // Wildcard ?
    try std.testing.expect(globMatch("doc?", "doc1"));
    try std.testing.expect(globMatch("doc?", "docA"));
    try std.testing.expect(!globMatch("doc?", "doc12"));

    // Mixed
    try std.testing.expect(globMatch("*.embedding.*", "field.embedding.vector"));
    try std.testing.expect(!globMatch("*.embedding.*", "field.text.vector"));
}

test "dynamic template field resolution" {
    const templates = [_]DynamicTemplate{
        .{
            .name = "embeddings",
            .match_pattern = "*_embedding",
            .mapping = .{ .field_type = .embedding, .doc_values = true },
        },
        .{
            .name = "keywords",
            .match_pattern = "*_id",
            .mapping = .{ .field_type = .keyword },
        },
    };

    const schema = TableSchema{
        .dynamic_templates = &templates,
        .enforce_types = true,
    };

    const emb = resolveFieldType(schema, "title_embedding");
    try std.testing.expect(emb != null);
    try std.testing.expectEqual(AntflyType.embedding, emb.?.field_type);
    try std.testing.expect(emb.?.doc_values);

    const kw = resolveFieldType(schema, "user_id");
    try std.testing.expect(kw != null);
    try std.testing.expectEqual(AntflyType.keyword, kw.?.field_type);

    const unknown = resolveFieldType(schema, "random_field");
    try std.testing.expect(unknown == null);

    // Validation: enforce_types rejects unknown fields
    const result = validateFields(schema, &.{"random_field"});
    try std.testing.expectError(error.UnknownFieldType, result);

    // Known fields pass validation
    try validateFields(schema, &.{"title_embedding"});
}

test "dynamic template selector and mapping-option resolution" {
    const templates = [_]DynamicTemplate{
        .{
            .name = "dates",
            .match_pattern = "*_at",
            .unmatch_pattern = "skip_*",
            .path_match = "meta.*",
            .path_unmatch = "meta.private.*",
            .match_mapping_type = "date",
            .mapping = .{
                .field_type = .datetime,
                .do_index = false,
                .store = false,
                .doc_values = true,
                .include_in_all = false,
                .analyzer = "keyword",
            },
        },
        .{
            .name = "keywords",
            .path_match = "meta.tags.*",
            .match_mapping_type = "string",
            .mapping = .{
                .field_type = .keyword,
                .include_in_all = true,
                .analyzer = "keyword",
            },
        },
    };

    const schema = TableSchema{ .dynamic_templates = &templates };

    const created = resolveFieldTypeForValue(schema, "meta.created_at", .{ .string = "2026-01-03T00:00:00Z" });
    try std.testing.expect(created != null);
    try std.testing.expectEqual(AntflyType.datetime, created.?.field_type);
    try std.testing.expect(!created.?.do_index);
    try std.testing.expect(created.?.doc_values);
    try std.testing.expectEqualStrings("keyword", created.?.analyzer);

    try std.testing.expect(resolveFieldTypeForValue(schema, "meta.skip_created_at", .{ .string = "2026-01-03T00:00:00Z" }) == null);
    try std.testing.expect(resolveFieldTypeForValue(schema, "meta.private.created_at", .{ .string = "2026-01-03T00:00:00Z" }) == null);
    try std.testing.expect(resolveFieldTypeForValue(schema, "meta.created_at", .{ .string = "not-a-date" }) == null);

    const tag = resolveFieldTypeForValue(schema, "meta.tags.primary", .{ .string = "alpha" });
    try std.testing.expect(tag != null);
    try std.testing.expectEqual(AntflyType.keyword, tag.?.field_type);
    try std.testing.expect(tag.?.include_in_all);
}
