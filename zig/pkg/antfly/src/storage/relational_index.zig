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

//! Schema-independent relational index and constraint contracts.
//! Ported from combine-pr-141-143-144 at 79644dfa1605e8da0f486d021d1c1393577d6265.
//! Callers own immutable schema snapshots and durable publication; this module
//! makes no DB, SQL, catalog-routing, or physical row-format assumptions.

const std = @import("std");

/// Store-local immutable physical identity. A catalog revision allocates one
/// slot per changed/new index; unchanged definitions retain their original ID.
/// Names remain in the catalog, not in every forward and reverse row key.
pub const RelationalIndexId = struct {
    generation: u64,
    slot: u32,
    pub const encoded_len = 12;

    pub fn encode(self: RelationalIndexId) [encoded_len]u8 {
        var bytes: [encoded_len]u8 = undefined;
        std.mem.writeInt(u64, bytes[0..8], self.generation, .big);
        std.mem.writeInt(u32, bytes[8..12], self.slot, .big);
        return bytes;
    }

    pub fn decode(bytes: []const u8) !RelationalIndexId {
        if (bytes.len != encoded_len) return error.InvalidRelationalIndexId;
        const generation = std.mem.readInt(u64, bytes[0..8], .big);
        if (generation == 0) return error.InvalidRelationalIndexId;
        return .{ .generation = generation, .slot = std.mem.readInt(u32, bytes[8..12], .big) };
    }

    pub fn mapKey(self: RelationalIndexId) u128 {
        return (@as(u128, self.generation) << 32) | self.slot;
    }
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
    column: []const u8 = "",
    expression_json: ?[]const u8 = null,
    result_type: ?@import("schema.zig").RelationalColumnType = null,
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

pub const RelationalIndexGenerationComponents = struct {
    dictionary: bool = true,
    fact: bool = true,
    path: bool = true,
    postings: bool = true,

    pub fn allReady(self: RelationalIndexGenerationComponents) bool {
        return self.dictionary and self.fact and self.path and self.postings;
    }

    pub fn merged(self: RelationalIndexGenerationComponents, other: RelationalIndexGenerationComponents) RelationalIndexGenerationComponents {
        return .{
            .dictionary = self.dictionary or other.dictionary,
            .fact = self.fact or other.fact,
            .path = self.path or other.path,
            .postings = self.postings or other.postings,
        };
    }

    pub fn contains(self: RelationalIndexGenerationComponents, component: RelationalIndexGenerationComponent) bool {
        return switch (component) {
            .dictionary => self.dictionary,
            .fact => self.fact,
            .path => self.path,
            .postings => self.postings,
        };
    }

    pub fn markReady(self: *RelationalIndexGenerationComponents, component: RelationalIndexGenerationComponent) void {
        switch (component) {
            .dictionary => self.dictionary = true,
            .fact => self.fact = true,
            .path => self.path = true,
            .postings => self.postings = true,
        }
    }
};

pub const RelationalIndexGenerationComponent = enum(u8) {
    dictionary = 0,
    fact = 1,
    path = 2,
    postings = 3,
};

pub const relational_index_generation_component_order = [_]RelationalIndexGenerationComponent{
    .fact,
    .path,
    .dictionary,
    .postings,
};

pub const relational_index_generation_components_none = RelationalIndexGenerationComponents{
    .dictionary = false,
    .fact = false,
    .path = false,
    .postings = false,
};

pub const RelationalIndexGenerationComponentCursors = struct {
    dictionary: ?[]const u8 = null,
    fact: ?[]const u8 = null,
    path: ?[]const u8 = null,
    postings: ?[]const u8 = null,

    pub fn get(self: RelationalIndexGenerationComponentCursors, component: RelationalIndexGenerationComponent) ?[]const u8 {
        return switch (component) {
            .dictionary => self.dictionary,
            .fact => self.fact,
            .path => self.path,
            .postings => self.postings,
        };
    }
};

pub const RelationalIndexRebuildLease = struct {
    owner_range: RelationalIndexOwnerRange = .{},
    holder: []const u8,
    cursor: []const u8,
    expires_at_ms: u64,
    generation: u64,
};

pub const RelationalIndexRebuildLeaseStatus = enum {
    live,
    expired,
    wrong_generation,
    missing_holder,
    missing_cursor,
    malformed,
};

pub const RelationalRangeMovementKind = enum {
    split,
    merge,
    movement,
};

pub const RelationalIndexGenerationRecord = struct {
    generation: u64,
    owner_ranges: []const RelationalIndexOwnerRange = &.{},
    lifecycle: RelationalIndexLifecycle = .ready,
    lag: u64 = 0,
    failure_reason: ?[]const u8 = null,
    ready_watermark: u64 = 0,
    rebuild_cursor: ?[]const u8 = null,
    components: RelationalIndexGenerationComponents = .{},
    component_cursors: RelationalIndexGenerationComponentCursors = .{},
    rebuild_leases: []const RelationalIndexRebuildLease = &.{},
};

pub const RelationalIndexGenerationPhaseWork = struct {
    component: RelationalIndexGenerationComponent,
    cursor: ?[]const u8,
};

pub const RelationalIndexGenerationPhaseDecision = union(enum) {
    work: RelationalIndexGenerationPhaseWork,
    complete,
    blocked,
    malformed,
};

pub const RelationalIndexDropPhaseWork = struct {
    component: ?RelationalIndexGenerationComponent = null,
    cursor: ?[]const u8 = null,
};

pub const RelationalIndexDropPhaseDecision = union(enum) {
    work: RelationalIndexDropPhaseWork,
    complete,
    blocked,
    malformed,
};

/// Select the first incomplete algebraic rebuild phase. A cursor is valid only
/// on that phase: later-phase cursors would violate the durable phase order and
/// are rejected rather than silently replayed out of order. The caller still
/// owns lifecycle transitions from stale/rebuild-required into building.
pub fn relationalIndexGenerationPhaseDecision(record: RelationalIndexGenerationRecord) RelationalIndexGenerationPhaseDecision {
    for (relational_index_generation_component_order) |component| {
        if (record.components.contains(component) and record.component_cursors.get(component) != null) return .malformed;
    }

    switch (record.lifecycle) {
        .ready => return if (record.components.allReady()) .complete else .malformed,
        .building, .catching_up, .failed => {},
        .invalid, .dropping, .stale, .rebuild_required => return .blocked,
    }

    if (record.components.allReady()) return .complete;

    var active_component: ?RelationalIndexGenerationComponent = null;
    for (relational_index_generation_component_order) |component| {
        if (record.components.contains(component)) continue;
        if (active_component == null) {
            active_component = component;
        } else if (record.component_cursors.get(component) != null) {
            return .malformed;
        }
    }
    const component = active_component orelse unreachable;
    return .{ .work = .{ .component = component, .cursor = record.component_cursors.get(component) } };
}

/// Interpret generation progress as durable artifact-deletion progress. Drop
/// uses the same cursor storage as rebuild because the lifecycle makes their
/// meaning disjoint: text search has one segment phase in `rebuild_cursor`,
/// while algebraic cleanup follows the normal component order.
pub fn relationalIndexDropPhaseDecision(index: RelationalIndex) RelationalIndexDropPhaseDecision {
    const record = index.generation_record orelse return .malformed;
    if (index.generation == 0 or record.generation != index.generation) return .malformed;
    if (index.lifecycle != .dropping or record.lifecycle != .dropping) return .blocked;
    if (index.owner_ranges.len != 0 and !relationalIndexOwnerRangeSlicesEqual(index.owner_ranges, record.owner_ranges)) return .malformed;

    switch (index.access_method) {
        .text_search => {
            if (record.component_cursors.dictionary != null or
                record.component_cursors.fact != null or
                record.component_cursors.path != null or
                record.component_cursors.postings != null)
            {
                return .malformed;
            }
            if (record.components.allReady()) {
                return if (record.rebuild_cursor == null) .complete else .malformed;
            }
            if (record.components.dictionary or record.components.fact or record.components.path or record.components.postings) return .malformed;
            return .{ .work = .{ .cursor = record.rebuild_cursor } };
        },
        .algebraic_filter => {
            if (record.rebuild_cursor != null) return .malformed;
            for (relational_index_generation_component_order) |component| {
                if (record.components.contains(component)) {
                    if (record.component_cursors.get(component) != null) return .malformed;
                    continue;
                }
                var later = false;
                for (relational_index_generation_component_order) |candidate| {
                    if (candidate == component) {
                        later = true;
                        continue;
                    }
                    if (!later) continue;
                    if (record.components.contains(candidate) or record.component_cursors.get(candidate) != null) return .malformed;
                }
                return .{ .work = .{ .component = component, .cursor = record.component_cursors.get(component) } };
            }
            return .complete;
        },
        .scalar_column, .ordered_tuple => return .blocked,
    }
}

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

/// Immutable index definition. Progress, leases, readiness, and placement belong
/// to the generation record, not to the definition rewritten by schema changes.
pub const RelationalIndexDefinition = struct {
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
    where: []const UniquePredicate = &.{},
    where_expressions: []const RelationalRowsExpressionCondition = &.{},

    pub fn fromIndex(index: RelationalIndex) RelationalIndexDefinition {
        var result: RelationalIndexDefinition = undefined;
        inline for (std.meta.fields(RelationalIndexDefinition)) |field|
            @field(result, field.name) = @field(index, field.name);
        return result;
    }
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
    return relationalIndexQueryBlockReason(index) == null;
}

pub fn relationalIndexWriteMaintenanceAllowed(index: RelationalIndex) bool {
    return relationalIndexWriteMaintenanceBlockReason(index) == null;
}

pub fn relationalIndexQueryBlockReason(index: RelationalIndex) ?[]const u8 {
    switch (index.access_method) {
        .scalar_column => {
            if (index.generation_record != null) return "relational_generation_malformed_record";
            if (index.lifecycle == .building or index.lifecycle == .catching_up) return "relational_generation_catch_up";
            return relationalWriteLifecycleBlockReason(index.lifecycle);
        },
        .ordered_tuple, .text_search, .algebraic_filter => {
            const record = index.generation_record orelse return "relational_generation_missing_record";
            if (index.generation == 0 or record.generation != index.generation) return "relational_generation_mismatch";
            if (record.lifecycle != index.lifecycle) return "relational_generation_malformed_record";
            if (index.owner_ranges.len != 0 and !relationalIndexOwnerRangeSlicesEqual(index.owner_ranges, record.owner_ranges)) return "relational_generation_owner_ranges_mismatch";
            if (record.lifecycle != .ready) {
                return switch (record.lifecycle) {
                    .building, .catching_up => "relational_generation_catch_up",
                    .stale => "relational_generation_stale",
                    .rebuild_required => "relational_generation_rebuild_required",
                    .failed => "relational_generation_failed",
                    .invalid => "relational_generation_invalid",
                    .dropping => "relational_generation_dropping",
                    .ready => unreachable,
                };
            }
            if (record.lag != 0) return "relational_generation_lag";
            if (index.access_method == .algebraic_filter and !record.components.allReady()) return "relational_generation_components_incomplete";
            return null;
        },
    }
}
pub fn relationalIndexWriteMaintenanceBlockReason(index: RelationalIndex) ?[]const u8 {
    switch (index.access_method) {
        .scalar_column => {
            if (index.generation_record != null) return "relational_generation_malformed_record";
            return relationalWriteLifecycleBlockReason(index.lifecycle);
        },
        .ordered_tuple, .text_search, .algebraic_filter => {
            const record = index.generation_record orelse return "relational_generation_missing_record";
            if (index.generation == 0 or record.generation != index.generation) return "relational_generation_mismatch";
            if (record.lifecycle != index.lifecycle) return "relational_generation_malformed_record";
            if (index.owner_ranges.len != 0 and !relationalIndexOwnerRangeSlicesEqual(index.owner_ranges, record.owner_ranges)) return "relational_generation_owner_ranges_mismatch";
            if (relationalWriteLifecycleBlockReason(record.lifecycle)) |reason| return reason;
            if (record.lifecycle == .ready and index.access_method == .algebraic_filter and !record.components.allReady()) return "relational_generation_components_incomplete";
            return null;
        },
    }
}

fn relationalWriteLifecycleBlockReason(lifecycle: RelationalIndexLifecycle) ?[]const u8 {
    return switch (lifecycle) {
        .ready, .building, .catching_up => null,
        .stale => "relational_generation_stale",
        .rebuild_required => "relational_generation_rebuild_required",
        .failed => "relational_generation_failed",
        .invalid => "relational_generation_invalid",
        .dropping => "relational_generation_dropping",
    };
}

pub fn relationalIndexRangeMovementBlockReason(index: RelationalIndex, kind: RelationalRangeMovementKind, now_ms: u64) ?[]const u8 {
    _ = kind;
    const query_reason = relationalIndexQueryBlockReason(index) orelse {
        const record = switch (index.access_method) {
            .scalar_column => return null,
            .ordered_tuple, .text_search, .algebraic_filter => index.generation_record orelse return "relational_generation_missing_record",
        };
        for (record.rebuild_leases) |lease| {
            switch (relationalIndexRebuildLeaseStatus(record, lease, index.generation, now_ms)) {
                .live => return "relational_generation_rebuild_lease_held",
                .expired => {},
                .wrong_generation => return "relational_generation_rebuild_lease_wrong_generation",
                .missing_holder => return "relational_generation_rebuild_lease_missing_holder",
                .missing_cursor => return "relational_generation_rebuild_lease_missing_cursor",
                .malformed => return "relational_generation_rebuild_lease_malformed",
            }
        }
        return null;
    };
    return query_reason;
}

pub fn relationalIndexOwnerRangeSlicesEqual(
    lhs: []const RelationalIndexOwnerRange,
    rhs: []const RelationalIndexOwnerRange,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |left, right| {
        if (!relationalIndexOwnerRangeEqual(left, right)) return false;
    }
    return true;
}

fn relationalIndexOwnerRangeEqual(left: RelationalIndexOwnerRange, right: RelationalIndexOwnerRange) bool {
    if (!std.mem.eql(u8, left.start, right.start)) return false;
    if (!std.mem.eql(u8, left.end, right.end)) return false;
    if (left.placement_generation != right.placement_generation) return false;
    if (left.range_id == null and right.range_id != null) return false;
    if (left.range_id != null and right.range_id == null) return false;
    if (left.range_id) |left_id| {
        if (!std.mem.eql(u8, left_id, right.range_id.?)) return false;
    }
    return true;
}

fn relationalIndexOwnerRangesContain(ranges: []const RelationalIndexOwnerRange, range: RelationalIndexOwnerRange) bool {
    if (ranges.len == 0) {
        return range.start.len == 0 and range.end.len == 0 and range.range_id == null and range.placement_generation == 0;
    }
    for (ranges) |candidate| {
        if (relationalIndexOwnerRangeEqual(candidate, range)) return true;
    }
    return false;
}

pub fn relationalIndexRebuildLeaseStatus(
    record: RelationalIndexGenerationRecord,
    lease: RelationalIndexRebuildLease,
    expected_generation: u64,
    now_ms: u64,
) RelationalIndexRebuildLeaseStatus {
    if (lease.generation != expected_generation or lease.generation != record.generation) return .wrong_generation;
    if (lease.holder.len == 0) return .missing_holder;
    if (lease.cursor.len == 0) return .missing_cursor;
    if (lease.expires_at_ms == 0) return .malformed;
    if (!relationalIndexOwnerRangesContain(record.owner_ranges, lease.owner_range)) return .malformed;
    if (lease.expires_at_ms <= now_ms) return .expired;
    return .live;
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

pub const UniquePredicateOp = RelationalCheckOp;

pub const UniquePredicate = struct {
    field: []const u8,
    op: UniquePredicateOp,
    value_json: ?[]const u8 = null,
    collation: ?[]const u8 = null,
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
test "relational index gates agree across every access method and lifecycle" {
    inline for (std.meta.tags(RelationalIndexAccessMethod)) |method| {
        inline for (std.meta.tags(RelationalIndexLifecycle)) |lifecycle| {
            const index = RelationalIndex{
                .name = "by_value",
                .owner_kind = .table,
                .owner_name = relational_table_index_owner_name,
                .access_method = method,
                .generation = 7,
                .lifecycle = lifecycle,
                .generation_record = if (method == .scalar_column) null else .{
                    .generation = 7,
                    .lifecycle = lifecycle,
                },
            };
            try std.testing.expectEqual(lifecycle == .ready, relationalIndexQueryReady(index));
            try std.testing.expectEqual(lifecycle == .ready, relationalIndexQueryBlockReason(index) == null);
            try std.testing.expectEqual(lifecycle == .ready, relationalIndexRangeMovementBlockReason(index, .split, 1000) == null);
            try std.testing.expectEqual(
                lifecycle == .ready or lifecycle == .building or lifecycle == .catching_up,
                relationalIndexWriteMaintenanceAllowed(index),
            );
        }
    }
}

test "relational index generation gates reject malformed and lagging records" {
    var index = RelationalIndex{
        .name = "by_value",
        .owner_kind = .table,
        .owner_name = relational_table_index_owner_name,
        .access_method = .ordered_tuple,
        .generation = 7,
    };
    try std.testing.expect(!relationalIndexQueryReady(index));
    try std.testing.expect(!relationalIndexWriteMaintenanceAllowed(index));
    index.generation_record = .{ .generation = 6 };
    try std.testing.expect(!relationalIndexQueryReady(index));
    index.generation_record = .{ .generation = 7, .lag = 1 };
    try std.testing.expect(!relationalIndexQueryReady(index));
    // Foreground writes must still maintain an index while it catches up.
    try std.testing.expect(relationalIndexWriteMaintenanceAllowed(index));
    index.generation_record = .{ .generation = 7, .lifecycle = .building };
    try std.testing.expect(!relationalIndexQueryReady(index));
    try std.testing.expect(!relationalIndexWriteMaintenanceAllowed(index));
    index.generation_record = .{ .generation = 7 };
    try std.testing.expect(relationalIndexQueryReady(index));
    index.access_method = .scalar_column;
    try std.testing.expect(!relationalIndexQueryReady(index));
    try std.testing.expect(!relationalIndexWriteMaintenanceAllowed(index));
    index.generation_record = null;
    try std.testing.expect(relationalIndexQueryReady(index));
}

test "relational index drop phases fence generation and cursor order" {
    var index = RelationalIndex{
        .name = "by_value",
        .owner_kind = .table,
        .owner_name = relational_table_index_owner_name,
        .access_method = .algebraic_filter,
        .generation = 7,
        .lifecycle = .dropping,
        .generation_record = .{
            .generation = 7,
            .lifecycle = .dropping,
            .components = relational_index_generation_components_none,
        },
    };
    try std.testing.expectEqual(RelationalIndexGenerationComponent.fact, relationalIndexDropPhaseDecision(index).work.component.?);
    index.generation_record.?.component_cursors.path = "row:m";
    try std.testing.expect(relationalIndexDropPhaseDecision(index) == .malformed);
    index.generation_record.?.component_cursors.path = null;
    index.generation_record.?.components.fact = true;
    try std.testing.expectEqual(RelationalIndexGenerationComponent.path, relationalIndexDropPhaseDecision(index).work.component.?);
    index.generation_record.?.generation = 6;
    try std.testing.expect(relationalIndexDropPhaseDecision(index) == .malformed);
    index.generation_record.?.generation = 7;
    index.lifecycle = .ready;
    try std.testing.expect(relationalIndexDropPhaseDecision(index) == .blocked);
    index.lifecycle = .dropping;
    index.generation_record.?.components = .{};
    try std.testing.expect(relationalIndexDropPhaseDecision(index) == .complete);
    index.access_method = .text_search;
    index.generation_record.?.components = relational_index_generation_components_none;
    index.generation_record.?.rebuild_cursor = "segment:next";
    try std.testing.expectEqualStrings("segment:next", relationalIndexDropPhaseDecision(index).work.cursor.?);
    index.generation_record.?.components = .{};
    try std.testing.expect(relationalIndexDropPhaseDecision(index) == .malformed);
    index.generation_record.?.rebuild_cursor = null;
    try std.testing.expect(relationalIndexDropPhaseDecision(index) == .complete);
}

test "schema selects algebraic rebuild phase by durable generation case" {
    const Expected = enum { work, complete, blocked, malformed };
    const Case = struct {
        name: []const u8,
        record: RelationalIndexGenerationRecord,
        expected: Expected,
        expected_component: ?RelationalIndexGenerationComponent = null,
        expected_cursor: ?[]const u8 = null,
    };
    const cases = [_]Case{
        .{
            .name = "fresh rebuild starts with facts",
            .record = .{ .generation = 7, .lifecycle = .building, .components = relational_index_generation_components_none },
            .expected = .work,
            .expected_component = .fact,
        },
        .{
            .name = "fact only",
            .record = .{ .generation = 7, .lifecycle = .catching_up, .components = .{ .dictionary = true, .fact = false, .path = true, .postings = true } },
            .expected = .work,
            .expected_component = .fact,
        },
        .{
            .name = "path only",
            .record = .{ .generation = 7, .lifecycle = .building, .components = .{ .dictionary = true, .fact = true, .path = false, .postings = true } },
            .expected = .work,
            .expected_component = .path,
        },
        .{
            .name = "postings only",
            .record = .{ .generation = 7, .lifecycle = .catching_up, .components = .{ .dictionary = true, .fact = true, .path = true, .postings = false } },
            .expected = .work,
            .expected_component = .postings,
        },
        .{
            .name = "later phase cursor before facts is malformed",
            .record = .{ .generation = 7, .lifecycle = .building, .components = relational_index_generation_components_none, .component_cursors = .{ .path = "row:m" } },
            .expected = .malformed,
        },
        .{
            .name = "failed phase resumes",
            .record = .{ .generation = 7, .lifecycle = .failed, .components = .{ .dictionary = true, .fact = false, .path = false, .postings = false }, .component_cursors = .{ .fact = "row:f" } },
            .expected = .work,
            .expected_component = .fact,
            .expected_cursor = "row:f",
        },
        .{
            .name = "completed phase cannot retain cursor",
            .record = .{ .generation = 7, .lifecycle = .catching_up, .components = .{ .dictionary = true, .fact = false, .path = false, .postings = false }, .component_cursors = .{ .dictionary = "row:d" } },
            .expected = .malformed,
        },
        .{
            .name = "ready record requires all phases",
            .record = .{ .generation = 7, .lifecycle = .ready, .components = .{ .dictionary = true, .fact = true, .path = true, .postings = false } },
            .expected = .malformed,
        },
        .{
            .name = "completed record",
            .record = .{ .generation = 7, .lifecycle = .ready },
            .expected = .complete,
        },
        .{
            .name = "component build complete before catchup",
            .record = .{ .generation = 7, .lifecycle = .building },
            .expected = .complete,
        },
        .{
            .name = "rebuild required needs lifecycle transition",
            .record = .{ .generation = 7, .lifecycle = .rebuild_required, .components = relational_index_generation_components_none },
            .expected = .blocked,
        },
    };

    for (cases) |case| {
        const decision = relationalIndexGenerationPhaseDecision(case.record);
        switch (decision) {
            .work => |work| {
                try std.testing.expectEqual(Expected.work, case.expected);
                try std.testing.expectEqual(case.expected_component.?, work.component);
                try expectOptionalStringEqual(case.expected_cursor, work.cursor);
            },
            .complete => try std.testing.expectEqual(Expected.complete, case.expected),
            .blocked => try std.testing.expectEqual(Expected.blocked, case.expected),
            .malformed => try std.testing.expectEqual(Expected.malformed, case.expected),
        }
    }
}

fn expectOptionalStringEqual(expected: ?[]const u8, actual: ?[]const u8) !void {
    if (expected) |expected_value| {
        try std.testing.expect(actual != null);
        try std.testing.expectEqualStrings(expected_value, actual.?);
    } else {
        try std.testing.expect(actual == null);
    }
}

test "schema classifies owner-range rebuild leases by persisted row case" {
    const ranges = [_]RelationalIndexOwnerRange{.{
        .start = "row:a",
        .end = "row:z",
        .range_id = "range-a",
        .placement_generation = 3,
    }};
    const other_range = RelationalIndexOwnerRange{
        .start = "row:z",
        .end = "",
        .range_id = "range-z",
        .placement_generation = 3,
    };
    const record = RelationalIndexGenerationRecord{
        .generation = 7,
        .owner_ranges = &ranges,
        .lifecycle = .catching_up,
        .lag = 2,
        .ready_watermark = 10,
    };
    const Case = struct {
        name: []const u8,
        lease: RelationalIndexRebuildLease,
        expected_generation: u64 = 7,
        now_ms: u64 = 1000,
        expected: RelationalIndexRebuildLeaseStatus,
    };
    const cases = [_]Case{
        .{
            .name = "live",
            .lease = .{ .owner_range = ranges[0], .holder = "worker-a", .cursor = "row:m", .expires_at_ms = 1001, .generation = 7 },
            .expected = .live,
        },
        .{
            .name = "expired",
            .lease = .{ .owner_range = ranges[0], .holder = "worker-a", .cursor = "row:m", .expires_at_ms = 1000, .generation = 7 },
            .expected = .expired,
        },
        .{
            .name = "wrong generation",
            .lease = .{ .owner_range = ranges[0], .holder = "worker-a", .cursor = "row:m", .expires_at_ms = 1001, .generation = 6 },
            .expected = .wrong_generation,
        },
        .{
            .name = "missing holder",
            .lease = .{ .owner_range = ranges[0], .holder = "", .cursor = "row:m", .expires_at_ms = 1001, .generation = 7 },
            .expected = .missing_holder,
        },
        .{
            .name = "missing cursor",
            .lease = .{ .owner_range = ranges[0], .holder = "worker-a", .cursor = "", .expires_at_ms = 1001, .generation = 7 },
            .expected = .missing_cursor,
        },
        .{
            .name = "malformed expiry",
            .lease = .{ .owner_range = ranges[0], .holder = "worker-a", .cursor = "row:m", .expires_at_ms = 0, .generation = 7 },
            .expected = .malformed,
        },
        .{
            .name = "malformed owner range",
            .lease = .{ .owner_range = other_range, .holder = "worker-a", .cursor = "row:m", .expires_at_ms = 1001, .generation = 7 },
            .expected = .malformed,
        },
    };

    for (cases) |case| {
        try std.testing.expectEqual(
            case.expected,
            relationalIndexRebuildLeaseStatus(record, case.lease, case.expected_generation, case.now_ms),
        );
    }
}
