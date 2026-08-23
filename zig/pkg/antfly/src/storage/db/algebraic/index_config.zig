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

//! Pure algebraic-index configuration and validation contract. Physical index,
//! backend, cache, and document-store implementations must not be imported here.

const std = @import("std");
const adaptive = @import("adaptive.zig");
const algebra = @import("algebra.zig");
const cylinder = @import("cylinder.zig");
const hll = @import("hll.zig");
const join = @import("join.zig");
const law = @import("law.zig");

pub const FieldConfig = struct {
    name: []const u8,
    path: []const u8,
    type: []const u8,
};

pub const DynamicRule = struct {
    name: []const u8 = "",
    match: ?[]const u8 = null,
    unmatch: ?[]const u8 = null,
    path_match: ?[]const u8 = null,
    path_unmatch: ?[]const u8 = null,
    match_mapping_type: ?[]const u8 = null,
    type: []const u8 = "string",
};

pub const JoinConfig = struct {
    name: []const u8,
    left_fields: []const []const u8 = &.{},
    right_fields: []const []const u8 = &.{},
    left_type_field: ?[]const u8 = null,
    left_type_value: ?[]const u8 = null,
    right_type_field: ?[]const u8 = null,
    right_type_value: ?[]const u8 = null,
    left_time_field: ?[]const u8 = null,
    right_time_field: ?[]const u8 = null,
    temporal_bucket: ?[]const u8 = null,
    temporal_window_seconds: ?i64 = null,
    max_fanout: ?usize = null,
};

pub const MaterializationConfig = struct {
    name: []const u8,
    op: []const u8,
    group_by: []const []const u8 = &.{},
    measure: ?[]const u8 = null,
    time: ?[]const u8 = null,
    bucket: ?[]const u8 = null,
    join: ?[]const u8 = null,
    group_side: ?[]const u8 = null,
    measure_side: ?[]const u8 = null,
    implicit_query: bool = false,
    law: ?[]const u8 = null,
    axes: []const []const u8 = &.{},
};

pub const LawConfig = struct {
    name: []const u8,
    id: []const u8,
    structure: []const u8 = "",
    invertible: bool = false,
};

pub const HllCardinalityConfig = struct {
    name: []const u8,
    group_by: []const []const u8 = &.{},
    value_field: []const u8,
    precision: u8 = 0,
};

pub const max_hll_cardinality_materializations: usize = 64;

pub const AdaptiveConfig = struct {
    observe: bool = true,
    lazy_materialization: bool = false,
    dematerialization: bool = false,
    min_observations: u64 = 3,
    max_auto_materializations_per_index: u64 = 32,
    max_backfill_rows_per_tick: u64 = 10_000,
    min_estimated_scan_rows_saved: u64 = 1_000,
    dematerialize_after_observation_misses: u64 = 3,
    observation_decay_after_misses: u64 = 0,
    observation_decay_retain_percent: u8 = 50,
    path_profile_history_retention: u64 = 64,

    pub fn policy(self: AdaptiveConfig) adaptive.Policy {
        return .{
            .observe = self.observe,
            .lazy_materialization = self.lazy_materialization,
            .dematerialization = self.dematerialization,
            .min_observations = self.min_observations,
            .max_auto_materializations_per_index = self.max_auto_materializations_per_index,
            .max_backfill_rows_per_tick = self.max_backfill_rows_per_tick,
            .min_estimated_scan_rows_saved = self.min_estimated_scan_rows_saved,
            .dematerialize_after_observation_misses = self.dematerialize_after_observation_misses,
            .observation_decay_after_misses = self.observation_decay_after_misses,
            .observation_decay_retain_percent = self.observation_decay_retain_percent,
            .path_profile_history_retention = self.path_profile_history_retention,
        };
    }
};

pub const PathFactPolicyConfig = struct {
    allow_numeric_string_coercion: bool = true,
    allow_datetime_string_coercion: bool = true,
};

pub const Config = struct {
    version: u16 = 2,
    table: []const u8 = "",
    schema_version: u32 = 0,
    capability_fingerprint: []const u8 = "",
    capability_lifecycle_status: []const u8 = "current",
    capability_change_added_fields: u32 = 0,
    capability_change_removed_fields: u32 = 0,
    capability_change_changed_type_fields: u32 = 0,
    skipped_dynamic_fields: u32 = 0,
    skipped_complex_fields: u32 = 0,
    skipped_unbounded_fields: u32 = 0,
    group_fields: []const FieldConfig = &.{},
    measure_fields: []const FieldConfig = &.{},
    time_fields: []const FieldConfig = &.{},
    dynamic_field_rules: []const DynamicRule = &.{},
    dynamic_rules_backfill_pending: bool = false,
    laws: []const LawConfig = &.{},
    joins: []const JoinConfig = &.{},
    materializations: []const MaterializationConfig = &.{},
    hll_cardinalities: []const HllCardinalityConfig = &.{},
    adaptive: AdaptiveConfig = .{},
    pathfact_policy: PathFactPolicyConfig = .{},
    max_result_buckets: ?usize = null,
    max_planner_scan_rows: ?usize = null,
    max_batch_accumulator_entries: ?usize = null,
    max_cardinality_cache_bytes: ?usize = null,
    max_hll_contributions_per_document: usize = 4096,
    max_hll_contribution_bytes_per_document: usize = 8 * 1024 * 1024,
    max_distributed_hll_partial_bytes: usize = 2 * 1024 * 1024,
    max_hll_maintenance_rows_per_tick: u64 = 10_000,
    max_pending_hll_observation_entries: usize = 4096,
    max_pending_hll_observation_bytes: usize = 1024 * 1024,
    min_max_candidate_cache_size: ?usize = null,
    enable_temporal_range_pruning: bool = true,
};

pub fn validateConfig(cfg: Config) !void {
    if (cfg.version != 1 and cfg.version != 2) return error.InvalidAlgebraicConfig;
    if (cfg.version == 2) {
        if (cfg.table.len == 0 or cfg.schema_version == 0 or cfg.capability_fingerprint.len == 0)
            return error.InvalidAlgebraicConfig;
    }

    try validateUniqueFieldNames(cfg.group_fields);
    try validateUniqueFieldNames(cfg.measure_fields);
    try validateUniqueFieldNames(cfg.time_fields);
    for (cfg.group_fields) |field| try validateField(field);
    for (cfg.measure_fields) |field| try validateField(field);
    for (cfg.time_fields) |field| try validateField(field);

    for (cfg.joins, 0..) |join_cfg, i| {
        if (join_cfg.name.len == 0) return error.InvalidAlgebraicConfig;
        for (cfg.joins[0..i]) |prior| {
            if (std.mem.eql(u8, prior.name, join_cfg.name)) return error.InvalidAlgebraicConfig;
        }
        if (join_cfg.left_fields.len == 0 or join_cfg.left_fields.len != join_cfg.right_fields.len)
            return error.InvalidAlgebraicConfig;
        for (join_cfg.left_fields) |name| if (!fieldConfigExists(cfg.group_fields, name))
            return error.InvalidAlgebraicConfig;
        for (join_cfg.right_fields) |name| if (!fieldConfigExists(cfg.group_fields, name))
            return error.InvalidAlgebraicConfig;
        if ((join_cfg.left_type_field == null) != (join_cfg.left_type_value == null) or
            (join_cfg.right_type_field == null) != (join_cfg.right_type_value == null))
            return error.InvalidAlgebraicConfig;
        if (join_cfg.left_type_field) |name| if (fieldConfigRoleCount(cfg, name) != 1)
            return error.InvalidAlgebraicConfig;
        if (join_cfg.right_type_field) |name| if (fieldConfigRoleCount(cfg, name) != 1)
            return error.InvalidAlgebraicConfig;
        if (join_cfg.left_time_field) |name| if (!fieldConfigExists(cfg.time_fields, name))
            return error.InvalidAlgebraicConfig;
        if (join_cfg.right_time_field) |name| if (!fieldConfigExists(cfg.time_fields, name))
            return error.InvalidAlgebraicConfig;
        try join.validateTemporal(join_cfg.left_time_field, join_cfg.right_time_field, join_cfg.temporal_bucket, join_cfg.temporal_window_seconds);
        if (join_cfg.max_fanout) |value| if (value == 0) return error.InvalidAlgebraicConfig;
    }

    for (cfg.laws, 0..) |law_cfg, i| {
        if (law_cfg.name.len == 0) return error.InvalidAlgebraicConfig;
        for (cfg.laws[0..i]) |prior| {
            if (std.mem.eql(u8, prior.name, law_cfg.name)) return error.InvalidAlgebraicConfig;
        }
        const law_id = law.Id.parse(law_cfg.id) orelse return error.InvalidAlgebraicConfig;
        if (law_cfg.structure.len > 0) {
            const structure = std.meta.stringToEnum(law.Structure, law_cfg.structure) orelse return error.InvalidAlgebraicConfig;
            if (structure != law.descriptor(law_id).structure) return error.InvalidAlgebraicConfig;
        }
        if (law_cfg.invertible and !law.descriptor(law_id).invertible) return error.InvalidAlgebraicConfig;
    }

    for (cfg.materializations, 0..) |mat, i| {
        if (mat.name.len == 0) return error.InvalidAlgebraicConfig;
        for (cfg.materializations[0..i]) |prior| {
            if (std.mem.eql(u8, prior.name, mat.name)) return error.InvalidAlgebraicConfig;
        }
        const op = algebra.Op.parse(mat.op) orelse return error.InvalidAlgebraicConfig;
        const implicit_law = law.fromOp(op);
        if (mat.law) |law_name| {
            if ((lawConfigId(cfg, law_name) orelse return error.InvalidAlgebraicConfig) != implicit_law)
                return error.InvalidAlgebraicConfig;
        }
        _ = law.descriptor(implicit_law);
        const group_fields = if (mat.axes.len > 0) mat.axes else mat.group_by;
        for (mat.group_by) |name| if (!fieldConfigExists(cfg.group_fields, name))
            return error.InvalidAlgebraicConfig;
        for (group_fields) |name| if (!fieldConfigExists(cfg.group_fields, name))
            return error.InvalidAlgebraicConfig;
        if (mat.measure) |name| {
            if (!fieldConfigExists(cfg.measure_fields, name)) return error.InvalidAlgebraicConfig;
        } else if (op != .count) return error.InvalidAlgebraicConfig;
        if (mat.time) |name| {
            if (!fieldConfigExists(cfg.time_fields, name)) return error.InvalidAlgebraicConfig;
            if (cylinder.Bucket.parse(mat.bucket orelse return error.InvalidAlgebraicConfig) == null)
                return error.InvalidAlgebraicConfig;
        } else if (mat.bucket != null) return error.InvalidAlgebraicConfig;
        if (mat.join) |name| {
            if (!joinConfigExists(cfg.joins, name) or mat.group_side == null or mat.measure_side == null)
                return error.InvalidAlgebraicConfig;
        } else if (mat.group_side != null or mat.measure_side != null) return error.InvalidAlgebraicConfig;
        if (mat.implicit_query) {
            const join_cfg = joinConfigForValidation(cfg.joins, mat.join orelse return error.InvalidAlgebraicConfig) orelse
                return error.InvalidAlgebraicConfig;
            if (!join.implicitQueryMaterializationProof(join_cfg, mat).safe()) return error.InvalidAlgebraicConfig;
        }
        if (mat.group_side) |side| if (!isJoinSide(side)) return error.InvalidAlgebraicConfig;
        if (mat.measure_side) |side| if (!isJoinSide(side)) return error.InvalidAlgebraicConfig;
    }
    if (cfg.hll_cardinalities.len > max_hll_cardinality_materializations) return error.InvalidAlgebraicConfig;
    for (cfg.hll_cardinalities, 0..) |hcfg, i| {
        if (hcfg.name.len == 0 or hcfg.value_field.len == 0) return error.InvalidAlgebraicConfig;
        if (!hllGroupFieldResolvable(cfg, hcfg.value_field)) return error.InvalidAlgebraicConfig;
        if (hcfg.precision != 0 and (hcfg.precision < hll.min_precision or hcfg.precision > hll.max_precision))
            return error.InvalidAlgebraicConfig;
        for (cfg.hll_cardinalities[0..i]) |prior| {
            if (std.mem.eql(u8, prior.name, hcfg.name)) return error.InvalidAlgebraicConfig;
        }
        for (hcfg.group_by, 0..) |axis, axis_idx| {
            if (axis.len == 0 or !hllGroupFieldResolvable(cfg, axis)) return error.InvalidAlgebraicConfig;
            for (hcfg.group_by[0..axis_idx]) |prior_axis| {
                if (std.mem.eql(u8, prior_axis, axis)) return error.InvalidAlgebraicConfig;
            }
        }
    }
    if (cfg.adaptive.observation_decay_retain_percent > 100) return error.InvalidAlgebraicConfig;
    if (cfg.max_hll_contributions_per_document == 0 or
        cfg.max_hll_contribution_bytes_per_document == 0 or
        cfg.max_distributed_hll_partial_bytes == 0 or
        cfg.max_hll_maintenance_rows_per_tick == 0 or
        cfg.max_pending_hll_observation_entries == 0 or
        cfg.max_pending_hll_observation_bytes == 0)
        return error.InvalidAlgebraicConfig;
    for (cfg.dynamic_field_rules) |rule| {
        if (rule.type.len == 0 or dynamicRuleRoleMask(rule.type) == 0) return error.InvalidAlgebraicConfig;
        if (rule.match == null and rule.path_match == null) return error.InvalidAlgebraicConfig;
    }
}

fn hllGroupFieldResolvable(cfg: Config, query_field: []const u8) bool {
    if (fieldConfigExists(cfg.group_fields, query_field)) return true;
    const field_name = fieldNameFromQueryPath(query_field);
    var resolved_type: ?[]const u8 = null;
    for (cfg.dynamic_field_rules) |rule| {
        if (!dynamicRuleNamePathMatches(rule, query_field, field_name)) continue;
        if (rule.match_mapping_type != null) return false;
        if (resolved_type) |prior| {
            if (!std.mem.eql(u8, prior, rule.type)) return false;
        } else {
            resolved_type = rule.type;
        }
    }
    return if (resolved_type) |scalar_type| dynamicRuleRoleMask(scalar_type) & role_group != 0 else false;
}

const role_group: u3 = 0b001;
const role_measure: u3 = 0b010;
const role_time: u3 = 0b100;

fn dynamicRuleRoleMask(scalar_type: []const u8) u3 {
    if (std.mem.eql(u8, scalar_type, "integer") or std.mem.eql(u8, scalar_type, "number"))
        return role_group | role_measure;
    if (std.mem.eql(u8, scalar_type, "datetime")) return role_group | role_time;
    if (std.mem.eql(u8, scalar_type, "string") or std.mem.eql(u8, scalar_type, "boolean")) return role_group;
    return 0;
}

fn dynamicRuleNamePathMatches(rule: DynamicRule, path: []const u8, field_name: []const u8) bool {
    if (rule.match) |pattern| if (!globMatch(pattern, field_name)) return false;
    if (rule.unmatch) |pattern| if (globMatch(pattern, field_name)) return false;
    if (rule.path_match) |pattern| if (!globMatch(pattern, path)) return false;
    if (rule.path_unmatch) |pattern| if (globMatch(pattern, path)) return false;
    return true;
}

fn fieldNameFromQueryPath(path: []const u8) []const u8 {
    const idx = std.mem.lastIndexOfScalar(u8, path, '.') orelse return path;
    return path[idx + 1 ..];
}

fn globMatch(pattern: []const u8, value: []const u8) bool {
    var pattern_index: usize = 0;
    var value_index: usize = 0;
    var star_index: ?usize = null;
    var star_value_index: usize = 0;
    while (value_index < value.len) {
        if (pattern_index < pattern.len and (pattern[pattern_index] == value[value_index] or pattern[pattern_index] == '?')) {
            pattern_index += 1;
            value_index += 1;
        } else if (pattern_index < pattern.len and pattern[pattern_index] == '*') {
            star_index = pattern_index;
            star_value_index = value_index;
            pattern_index += 1;
        } else if (star_index) |star| {
            pattern_index = star + 1;
            star_value_index += 1;
            value_index = star_value_index;
        } else return false;
    }
    while (pattern_index < pattern.len and pattern[pattern_index] == '*') pattern_index += 1;
    return pattern_index == pattern.len;
}

fn validateField(field: FieldConfig) !void {
    if (field.name.len == 0 or field.path.len == 0 or field.type.len == 0)
        return error.InvalidAlgebraicConfig;
}

fn validateUniqueFieldNames(fields: []const FieldConfig) !void {
    for (fields, 0..) |field, i| for (fields[0..i]) |prior| {
        if (std.mem.eql(u8, prior.name, field.name)) return error.InvalidAlgebraicConfig;
    };
}

fn fieldConfigExists(fields: []const FieldConfig, name: []const u8) bool {
    for (fields) |field| if (std.mem.eql(u8, field.name, name)) return true;
    return false;
}

fn joinConfigExists(joins: []const JoinConfig, name: []const u8) bool {
    return joinConfigForValidation(joins, name) != null;
}

fn joinConfigForValidation(joins: []const JoinConfig, name: []const u8) ?JoinConfig {
    for (joins) |join_cfg| if (std.mem.eql(u8, join_cfg.name, name)) return join_cfg;
    return null;
}

pub fn lawConfigId(cfg: Config, name: []const u8) ?law.Id {
    for (cfg.laws) |law_cfg| if (std.mem.eql(u8, law_cfg.name, name)) return law.Id.parse(law_cfg.id);
    return null;
}

fn fieldConfigRoleCount(cfg: Config, name: []const u8) usize {
    return @as(usize, @intFromBool(fieldConfigExists(cfg.group_fields, name))) +
        @as(usize, @intFromBool(fieldConfigExists(cfg.measure_fields, name))) +
        @as(usize, @intFromBool(fieldConfigExists(cfg.time_fields, name)));
}

fn isJoinSide(side: []const u8) bool {
    return std.mem.eql(u8, side, "left") or std.mem.eql(u8, side, "right");
}

test "algebraic index config validation retains exact failure identity" {
    try std.testing.expectError(error.InvalidAlgebraicConfig, validateConfig(.{}));
    try validateConfig(.{
        .table = "docs",
        .schema_version = 1,
        .capability_fingerprint = "sha256:test",
    });
}
