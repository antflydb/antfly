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
const Allocator = std.mem.Allocator;
const cylinder = @import("cylinder.zig");
const law = @import("law.zig");
const token = @import("token.zig");

pub const Side = enum {
    left,
    right,

    pub fn tag(self: Side) []const u8 {
        return switch (self) {
            .left => "l",
            .right => "r",
        };
    }

    pub fn opposite(self: Side) Side {
        return switch (self) {
            .left => .right,
            .right => .left,
        };
    }
};

pub fn sideFromName(name: []const u8) ?Side {
    if (std.mem.eql(u8, name, "left")) return .left;
    if (std.mem.eql(u8, name, "right")) return .right;
    return null;
}

pub fn validSideName(name: []const u8) bool {
    return sideFromName(name) != null;
}

pub const TemporalMode = enum {
    none,
    bucket,
    window,
    bucket_window,
};

pub fn temporalMode(bucket: ?[]const u8, window_seconds: ?i64) TemporalMode {
    return if (bucket != null and window_seconds != null)
        .bucket_window
    else if (bucket != null)
        .bucket
    else if (window_seconds != null)
        .window
    else
        .none;
}

pub fn hasCompleteSidePredicate(type_field: ?[]const u8, type_value: ?[]const u8) bool {
    return type_field != null and type_value != null;
}

pub const ImplicitQueryRejectReason = enum {
    missing_join,
    missing_group_side,
    missing_measure_side,
    missing_left_type_predicate,
    missing_right_type_predicate,
};

pub const ImplicitQueryProof = union(enum) {
    proven,
    rejected: ImplicitQueryRejectReason,

    pub fn safe(self: ImplicitQueryProof) bool {
        return self == .proven;
    }
};

pub fn implicitQueryMaterializationProof(join_cfg: anytype, mat: anytype) ImplicitQueryProof {
    if (mat.join == null) return .{ .rejected = .missing_join };
    if (mat.group_side == null) return .{ .rejected = .missing_group_side };
    if (mat.measure_side == null) return .{ .rejected = .missing_measure_side };
    if (!hasCompleteSidePredicate(join_cfg.left_type_field, join_cfg.left_type_value)) return .{ .rejected = .missing_left_type_predicate };
    if (!hasCompleteSidePredicate(join_cfg.right_type_field, join_cfg.right_type_value)) return .{ .rejected = .missing_right_type_predicate };
    return .proven;
}

pub fn implicitQueryMaterializationSafe(join_cfg: anytype, mat: anytype) bool {
    return implicitQueryMaterializationProof(join_cfg, mat).safe();
}

pub const NamedQueryRejectReason = enum {
    missing_join,
    missing_group_side,
    missing_measure_side,
};

pub const NamedQueryProof = union(enum) {
    proven,
    rejected: NamedQueryRejectReason,

    pub fn safe(self: NamedQueryProof) bool {
        return self == .proven;
    }
};

pub fn namedQueryMaterializationProof(mat: anytype) NamedQueryProof {
    if (mat.join == null) return .{ .rejected = .missing_join };
    if (mat.group_side == null) return .{ .rejected = .missing_group_side };
    if (mat.measure_side == null) return .{ .rejected = .missing_measure_side };
    return .proven;
}

pub fn namedQueryMaterializationSafe(mat: anytype) bool {
    return namedQueryMaterializationProof(mat).safe();
}

pub const ExplicitQueryRejectReason = enum {
    missing_join,
    missing_query_group_side,
    missing_query_measure_side,
    invalid_query_group_side,
    invalid_query_measure_side,
    missing_materialization_group_side,
    missing_materialization_measure_side,
    group_side_mismatch,
    measure_side_mismatch,
    temporal_mode_mismatch,
};

pub const ExplicitQueryProof = union(enum) {
    proven,
    rejected: ExplicitQueryRejectReason,

    pub fn safe(self: ExplicitQueryProof) bool {
        return self == .proven;
    }
};

pub fn explicitQueryMaterializationProof(join_cfg: anytype, mat: anytype, join_ref: anytype) ExplicitQueryProof {
    if (mat.join == null) return .{ .rejected = .missing_join };
    const query_group_side = join_ref.group_side orelse return .{ .rejected = .missing_query_group_side };
    const query_measure_side = join_ref.measure_side orelse return .{ .rejected = .missing_query_measure_side };
    if (!validSideName(query_group_side)) return .{ .rejected = .invalid_query_group_side };
    if (!validSideName(query_measure_side)) return .{ .rejected = .invalid_query_measure_side };
    const mat_group_side = mat.group_side orelse return .{ .rejected = .missing_materialization_group_side };
    const mat_measure_side = mat.measure_side orelse return .{ .rejected = .missing_materialization_measure_side };
    if (!std.mem.eql(u8, mat_group_side, query_group_side)) return .{ .rejected = .group_side_mismatch };
    if (!std.mem.eql(u8, mat_measure_side, query_measure_side)) return .{ .rejected = .measure_side_mismatch };
    if (!temporalModeProof(join_cfg, join_ref.kind).matches()) return .{ .rejected = .temporal_mode_mismatch };
    return .proven;
}

pub fn explicitQueryMaterializationSafe(join_cfg: anytype, mat: anytype, join_ref: anytype) bool {
    return explicitQueryMaterializationProof(join_cfg, mat, join_ref).safe();
}

pub const JoinRewriteKind = enum {
    predeclared_materialization,
    derived_distributive_fold,
};

pub const JoinRewriteOptions = struct {
    kind: JoinRewriteKind,
    law_id: ?law.Id = null,
    bounded_fanout: bool = false,
};

pub const JoinRewriteRejectReason = enum {
    missing_query_group_side,
    missing_query_measure_side,
    invalid_query_group_side,
    invalid_query_measure_side,
    temporal_mode_mismatch,
    missing_left_type_predicate,
    missing_right_type_predicate,
    missing_law,
    non_distributive_law,
    unbounded_fanout,
};

pub const JoinRewriteProof = union(enum) {
    proven,
    rejected: JoinRewriteRejectReason,

    pub fn safe(self: JoinRewriteProof) bool {
        return self == .proven;
    }
};

pub fn queryRewriteProof(join_cfg: anytype, join_ref: anytype, options: JoinRewriteOptions) JoinRewriteProof {
    const query_group_side = join_ref.group_side orelse return .{ .rejected = .missing_query_group_side };
    const query_measure_side = join_ref.measure_side orelse return .{ .rejected = .missing_query_measure_side };
    if (!validSideName(query_group_side)) return .{ .rejected = .invalid_query_group_side };
    if (!validSideName(query_measure_side)) return .{ .rejected = .invalid_query_measure_side };
    if (!temporalModeProof(join_cfg, join_ref.kind).matches()) return .{ .rejected = .temporal_mode_mismatch };

    switch (options.kind) {
        .predeclared_materialization => return .proven,
        .derived_distributive_fold => {},
    }

    if (!hasCompleteSidePredicate(join_cfg.left_type_field, join_cfg.left_type_value)) return .{ .rejected = .missing_left_type_predicate };
    if (!hasCompleteSidePredicate(join_cfg.right_type_field, join_cfg.right_type_value)) return .{ .rejected = .missing_right_type_predicate };
    const law_id = options.law_id orelse return .{ .rejected = .missing_law };
    if (!lawPreservesDerivedJoinFold(law_id)) return .{ .rejected = .non_distributive_law };
    if (!options.bounded_fanout) return .{ .rejected = .unbounded_fanout };
    return .proven;
}

pub fn lawPreservesDerivedJoinFold(law_id: law.Id) bool {
    return switch (law_id) {
        .count,
        .sum,
        .sumsquares,
        .min,
        .max,
        .bool_any,
        .bool_all,
        .set_union,
        .max_timestamp,
        .provenance_semiring,
        .avg,
        => true,
    };
}

pub const TemporalProof = union(enum) {
    proven: TemporalMode,
    rejected_mode_mismatch: struct {
        expected: TemporalMode,
        actual: TemporalMode,
    },

    pub fn matches(self: TemporalProof) bool {
        return self == .proven;
    }
};

pub fn temporalModeProof(join_cfg: anytype, mode: TemporalMode) TemporalProof {
    const actual = temporalMode(join_cfg.temporal_bucket, join_cfg.temporal_window_seconds);
    if (mode != actual) return .{ .rejected_mode_mismatch = .{ .expected = mode, .actual = actual } };
    return .{ .proven = actual };
}

pub fn temporalModeMatches(join_cfg: anytype, mode: TemporalMode) bool {
    return temporalModeProof(join_cfg, mode).matches();
}

pub fn validateTemporal(
    left_time_field: ?[]const u8,
    right_time_field: ?[]const u8,
    bucket: ?[]const u8,
    window_seconds: ?i64,
) !void {
    if ((left_time_field == null) != (right_time_field == null)) return error.InvalidAlgebraicConfig;
    if (bucket != null or window_seconds != null) {
        if (left_time_field == null or right_time_field == null) return error.InvalidAlgebraicConfig;
    }
    if (bucket) |value| {
        if (cylinder.Bucket.parse(value) == null) return error.InvalidAlgebraicConfig;
    }
    if (window_seconds) |value| {
        if (value <= 0) return error.InvalidAlgebraicConfig;
    }
}

pub fn compositeKeyAlloc(alloc: Allocator, values: []const []const u8) ![]u8 {
    return try token.canonicalTupleAlloc(alloc, values);
}

test "join composite keys are ordered tuples" {
    const alloc = std.testing.allocator;
    const values = [_][]const u8{ "tenant-a", "customer-1" };
    const key = try compositeKeyAlloc(alloc, values[0..]);
    defer alloc.free(key);
    try std.testing.expectEqualStrings("8:tenant-a|10:customer-1", key);
}

test "join proof gates implicit query materializations to side-disambiguated exact joins" {
    const JoinCfg = struct {
        left_type_field: ?[]const u8 = "kind",
        left_type_value: ?[]const u8 = "order",
        right_type_field: ?[]const u8 = "kind",
        right_type_value: ?[]const u8 = "customer",
        temporal_bucket: ?[]const u8 = null,
        temporal_window_seconds: ?i64 = null,
    };
    const Mat = struct {
        join: ?[]const u8 = "orders_customers",
        group_side: ?[]const u8 = "right",
        measure_side: ?[]const u8 = "left",
    };
    const JoinRef = struct {
        group_side: ?[]const u8 = "right",
        measure_side: ?[]const u8 = "left",
        kind: TemporalMode = .none,
    };

    try std.testing.expect(implicitQueryMaterializationSafe(JoinCfg{}, Mat{}));
    try std.testing.expectEqual(ImplicitQueryProof.proven, implicitQueryMaterializationProof(JoinCfg{}, Mat{}));
    try std.testing.expectEqual(ImplicitQueryProof{ .rejected = .missing_right_type_predicate }, implicitQueryMaterializationProof(JoinCfg{ .right_type_field = null }, Mat{}));
    try std.testing.expectEqual(ImplicitQueryProof{ .rejected = .missing_measure_side }, implicitQueryMaterializationProof(JoinCfg{}, Mat{ .measure_side = null }));
    try std.testing.expect(namedQueryMaterializationSafe(Mat{}));
    try std.testing.expectEqual(NamedQueryProof.proven, namedQueryMaterializationProof(Mat{}));
    try std.testing.expectEqual(NamedQueryProof{ .rejected = .missing_join }, namedQueryMaterializationProof(Mat{ .join = null }));
    try std.testing.expectEqual(NamedQueryProof{ .rejected = .missing_group_side }, namedQueryMaterializationProof(Mat{ .group_side = null }));
    try std.testing.expectEqual(NamedQueryProof{ .rejected = .missing_measure_side }, namedQueryMaterializationProof(Mat{ .measure_side = null }));
    try std.testing.expect(explicitQueryMaterializationSafe(JoinCfg{}, Mat{}, JoinRef{}));
    try std.testing.expectEqual(ExplicitQueryProof.proven, explicitQueryMaterializationProof(JoinCfg{}, Mat{}, JoinRef{}));
    try std.testing.expectEqual(ExplicitQueryProof{ .rejected = .missing_query_group_side }, explicitQueryMaterializationProof(JoinCfg{}, Mat{}, JoinRef{ .group_side = null }));
    try std.testing.expectEqual(ExplicitQueryProof{ .rejected = .invalid_query_group_side }, explicitQueryMaterializationProof(JoinCfg{}, Mat{}, JoinRef{ .group_side = "middle" }));
    try std.testing.expectEqual(ExplicitQueryProof{ .rejected = .invalid_query_measure_side }, explicitQueryMaterializationProof(JoinCfg{}, Mat{}, JoinRef{ .measure_side = "middle" }));
    try std.testing.expectEqual(ExplicitQueryProof{ .rejected = .missing_materialization_measure_side }, explicitQueryMaterializationProof(JoinCfg{}, Mat{ .measure_side = null }, JoinRef{}));
    try std.testing.expectEqual(ExplicitQueryProof{ .rejected = .group_side_mismatch }, explicitQueryMaterializationProof(JoinCfg{}, Mat{}, JoinRef{ .group_side = "left" }));
    try std.testing.expectEqual(ExplicitQueryProof{ .rejected = .temporal_mode_mismatch }, explicitQueryMaterializationProof(JoinCfg{ .temporal_bucket = "hour" }, Mat{}, JoinRef{}));
    try std.testing.expect(explicitQueryMaterializationSafe(JoinCfg{ .temporal_bucket = "hour" }, Mat{}, JoinRef{ .kind = .bucket }));
    try std.testing.expectEqual(JoinRewriteProof.proven, queryRewriteProof(JoinCfg{}, JoinRef{}, .{ .kind = .predeclared_materialization }));
    try std.testing.expectEqual(
        JoinRewriteProof{ .rejected = .temporal_mode_mismatch },
        queryRewriteProof(JoinCfg{ .temporal_window_seconds = 60 }, JoinRef{}, .{ .kind = .predeclared_materialization }),
    );
    try std.testing.expectEqual(
        JoinRewriteProof{ .rejected = .unbounded_fanout },
        queryRewriteProof(JoinCfg{}, JoinRef{}, .{ .kind = .derived_distributive_fold, .law_id = .sum }),
    );
    try std.testing.expectEqual(
        JoinRewriteProof.proven,
        queryRewriteProof(JoinCfg{}, JoinRef{}, .{ .kind = .derived_distributive_fold, .law_id = .avg, .bounded_fanout = true }),
    );
    try std.testing.expectEqual(
        JoinRewriteProof.proven,
        queryRewriteProof(JoinCfg{}, JoinRef{}, .{ .kind = .derived_distributive_fold, .law_id = .sum, .bounded_fanout = true }),
    );
    try std.testing.expect(temporalModeMatches(JoinCfg{}, .none));
    try std.testing.expect(temporalModeMatches(JoinCfg{ .temporal_bucket = "hour" }, .bucket));
    try std.testing.expect(!temporalModeMatches(JoinCfg{ .temporal_window_seconds = 60 }, .bucket));
    try std.testing.expectEqual(TemporalProof{ .proven = .bucket }, temporalModeProof(JoinCfg{ .temporal_bucket = "hour" }, .bucket));
    try std.testing.expectEqual(TemporalProof{ .rejected_mode_mismatch = .{ .expected = .bucket, .actual = .window } }, temporalModeProof(JoinCfg{ .temporal_window_seconds = 60 }, .bucket));
}
