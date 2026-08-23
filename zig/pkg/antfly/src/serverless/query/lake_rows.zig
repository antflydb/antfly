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

//! Lake-native query execution scaffold over RowSource. This is intentionally
//! small: it proves serverless row fragments, local relational batches, and
//! external lake batches can share one execution path, while allowing an
//! algebraic segment to satisfy repeated group-by aggregate workloads.

const std = @import("std");
const Allocator = std.mem.Allocator;
const algebraic_segment = @import("../algebraic_segment/mod.zig");
const aggregate_math = algebraic_segment.aggregate_math;
const base_source = @import("../manifest/base_source.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const source_binding = @import("../segment/source_binding.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const lake_sidecar_selection = @import("lake_sidecar_selection.zig");

pub const GroupByRequest = struct {
    group_column: []const u8,
    value_column: []const u8 = &.{},
    op: algebraic_segment.AggregateOp,
    materialized_source: ?MaterializedSourceRef = null,
    deleted_row_refs: []const rowsource.RowRef = &.{},
    deleted_row_filter: ?DeletedRowFilter = null,
    limits: AggregateLimits = .{},
};

pub const ExpressionAggregateRequest = struct {
    expressions: []const algebraic_segment.ExpressionSpec,
    materialized_source: ?MaterializedSourceRef = null,
    deleted_row_refs: []const rowsource.RowRef = &.{},
    deleted_row_filter: ?DeletedRowFilter = null,
    limits: AggregateLimits = .{},
};

pub const AggregateLimits = struct {
    max_batches: usize = 100_000,
    max_rows_examined: u64 = 100_000_000,
    max_groups: usize = 1_000_000,
    max_group_key_bytes: usize = 128 * 1024 * 1024,
    max_deleted_row_refs: usize = 1_000_000,
    max_expressions: usize = 1_024,

    pub fn validate(self: AggregateLimits) !void {
        if (self.max_batches == 0 or self.max_rows_examined == 0 or self.max_groups == 0 or
            self.max_group_key_bytes == 0 or self.max_deleted_row_refs == 0 or self.max_expressions == 0)
        {
            return error.InvalidLakeRowsAggregateLimits;
        }
    }
};

const AggregateBudget = struct {
    limits: AggregateLimits,
    batches: usize = 0,
    rows_examined: u64 = 0,
    group_key_bytes: usize = 0,

    fn admitBatch(self: *AggregateBudget, row_count: usize) !void {
        self.batches = std.math.add(usize, self.batches, 1) catch return error.LakeRowsAggregateBudgetExceeded;
        if (self.batches > self.limits.max_batches) return error.LakeRowsAggregateBudgetExceeded;
        self.rows_examined = std.math.add(u64, self.rows_examined, row_count) catch
            return error.LakeRowsAggregateBudgetExceeded;
        if (self.rows_examined > self.limits.max_rows_examined) return error.LakeRowsAggregateBudgetExceeded;
    }

    fn admitGroup(self: *AggregateBudget, group_count: usize, key_len: usize) !void {
        if (group_count >= self.limits.max_groups) return error.LakeRowsAggregateBudgetExceeded;
        self.group_key_bytes = std.math.add(usize, self.group_key_bytes, key_len) catch
            return error.LakeRowsAggregateBudgetExceeded;
        if (self.group_key_bytes > self.limits.max_group_key_bytes) return error.LakeRowsAggregateBudgetExceeded;
    }
};

pub const DeletedRowFilter = struct {
    ctx: *const anyopaque,
    contains_fn: *const fn (ctx: *const anyopaque, row_ref: rowsource.RowRef) bool,

    pub fn contains(self: DeletedRowFilter, row_ref: rowsource.RowRef) bool {
        return self.contains_fn(self.ctx, row_ref);
    }
};

pub const MaterializedSourceRef = struct {
    kind: rowsource.SourceKind,
    source_id: []const u8 = &.{},
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,

    pub fn validate(self: MaterializedSourceRef) !void {
        if (self.snapshot_id.len == 0) return error.InvalidLakeRowsQuery;
        if (self.schema_fingerprint.len == 0) return error.InvalidLakeRowsQuery;
        switch (self.kind) {
            .external_parquet, .external_iceberg, .external_lance => {
                if (self.source_id.len == 0) return error.InvalidLakeRowsQuery;
            },
            .relational_store, .json_materialized, .serverless_fragment => {},
        }
    }
};

pub const GroupResult = struct {
    key: []u8,
    value: algebraic_segment.AggregateValue,

    pub fn deinit(self: *GroupResult, alloc: Allocator) void {
        alloc.free(self.key);
        self.* = undefined;
    }
};

pub const GroupByResult = struct {
    groups: []GroupResult,
    source: enum { rowsource_scan, algebraic_segment },

    pub fn deinit(self: *GroupByResult, alloc: Allocator) void {
        for (self.groups) |*group| group.deinit(alloc);
        alloc.free(self.groups);
        self.* = undefined;
    }

    pub fn find(self: GroupByResult, key: []const u8) ?algebraic_segment.AggregateValue {
        for (self.groups) |group| {
            if (std.mem.eql(u8, group.key, key)) return group.value;
        }
        return null;
    }
};

pub const ExpressionResult = struct {
    name: []u8,
    value: algebraic_segment.AggregateValue,

    pub fn deinit(self: *ExpressionResult, alloc: Allocator) void {
        alloc.free(self.name);
        self.* = undefined;
    }
};

pub const ExpressionAggregateResult = struct {
    expressions: []ExpressionResult,
    source: enum { rowsource_scan, algebraic_expression },

    pub fn deinit(self: *ExpressionAggregateResult, alloc: Allocator) void {
        for (self.expressions) |*expression| expression.deinit(alloc);
        alloc.free(self.expressions);
        self.* = undefined;
    }

    pub fn find(self: ExpressionAggregateResult, name: []const u8) ?algebraic_segment.AggregateValue {
        for (self.expressions) |expression| {
            if (std.mem.eql(u8, expression.name, name)) return expression.value;
        }
        return null;
    }
};

pub const CellValue = union(rowsource.ColumnKind) {
    bytes: []u8,
    json: []u8,
    i64: i64,
    f64: f64,
    bool: bool,
    vector_f32: []f32,

    pub fn deinit(self: *CellValue, alloc: Allocator) void {
        switch (self.*) {
            .bytes => |value| alloc.free(value),
            .json => |value| alloc.free(value),
            .vector_f32 => |value| alloc.free(value),
            else => {},
        }
        self.* = undefined;
    }
};

pub const ProjectedCell = struct {
    name: []u8,
    value: ?CellValue,

    pub fn deinit(self: *ProjectedCell, alloc: Allocator) void {
        alloc.free(self.name);
        if (self.value) |*value| value.deinit(alloc);
        self.* = undefined;
    }
};

pub const ProjectedRow = struct {
    row_ref: rowsource.RowRef,
    cells: []ProjectedCell,
    owns_row_ref: bool = false,

    pub fn deinit(self: *ProjectedRow, alloc: Allocator) void {
        if (self.owns_row_ref) source_binding.freeOwnedRowRef(alloc, self.row_ref);
        for (self.cells) |*cell| cell.deinit(alloc);
        alloc.free(self.cells);
        self.* = undefined;
    }

    pub fn find(self: ProjectedRow, name: []const u8) ?ProjectedCell {
        for (self.cells) |cell| {
            if (std.mem.eql(u8, cell.name, name)) return cell;
        }
        return null;
    }

    pub fn allocatedBytes(self: ProjectedRow) usize {
        var total = std.math.add(
            usize,
            @sizeOf(ProjectedRow),
            std.math.mul(usize, self.cells.len, @sizeOf(ProjectedCell)) catch return std.math.maxInt(usize),
        ) catch return std.math.maxInt(usize);
        if (self.owns_row_ref) {
            total = std.math.add(usize, total, rowRefPayloadBytes(self.row_ref)) catch return std.math.maxInt(usize);
        }
        for (self.cells) |cell| {
            total = std.math.add(usize, total, cell.name.len) catch return std.math.maxInt(usize);
            if (cell.value) |value| {
                const payload_bytes = switch (value) {
                    .bytes => |bytes| bytes.len,
                    .json => |bytes| bytes.len,
                    .vector_f32 => |values| std.math.mul(usize, values.len, @sizeOf(f32)) catch return std.math.maxInt(usize),
                    .i64, .f64, .bool => 0,
                };
                total = std.math.add(usize, total, payload_bytes) catch return std.math.maxInt(usize);
            }
        }
        return total;
    }
};

pub const HydrateResult = struct {
    rows: []ProjectedRow,
    total: u32 = 0,

    pub fn deinit(self: *HydrateResult, alloc: Allocator) void {
        for (self.rows) |*row| row.deinit(alloc);
        alloc.free(self.rows);
        self.* = undefined;
    }
};

pub const PredicateOp = enum {
    eq_bytes,
    eq_i64,
    eq_f64,
    eq_bool,
};

pub const Predicate = struct {
    column: []const u8,
    op: PredicateOp,
    bytes_value: []const u8 = &.{},
    i64_value: i64 = 0,
    f64_value: f64 = 0,
    bool_value: bool = false,
};

pub const ScanRequest = struct {
    projected_columns: []const []const u8,
    predicate: ?Predicate = null,
    limit: ?usize = null,
    deleted_row_refs: []const rowsource.RowRef = &.{},
    deleted_row_filter: ?DeletedRowFilter = null,
    limits: ScanLimits = .{},
};

pub const ScanLimits = struct {
    max_batches: usize = 100_000,
    max_rows_examined: u64 = 100_000_000,
    max_matching_rows: u64 = std.math.maxInt(u32),
    max_materialized_rows: usize = 1_000_000,
    max_materialized_bytes: usize = 256 * 1024 * 1024,
    max_lookup_row_refs: usize = 1_000_000,

    pub fn validate(self: ScanLimits) !void {
        if (self.max_batches == 0 or self.max_rows_examined == 0 or self.max_matching_rows == 0 or
            self.max_materialized_rows == 0 or self.max_materialized_bytes == 0 or self.max_lookup_row_refs == 0 or
            self.max_matching_rows > std.math.maxInt(u32))
        {
            return error.InvalidLakeRowsScanLimits;
        }
    }
};

pub const ScanResult = HydrateResult;

pub const SidecarCandidateSet = struct {
    sidecar_name: []const u8,
    row_refs: []const rowsource.RowRef,
};

pub const SidecarScanRequest = struct {
    scan: ScanRequest,
    base_source: base_source.BaseSourceDescriptor,
    sidecars: []const sidecar_manifest.DeclaredArtifact = &.{},
    desired_sidecars: []const lake_sidecar_selection.DesiredSidecar = &.{},
    sidecar_policy: lake_sidecar_selection.Policy = .{},
    candidates: []const SidecarCandidateSet = &.{},
};

pub const SidecarScanSource = enum {
    rowsource_scan,
    sidecar_hydration,
};

pub const SidecarScanResult = struct {
    rows: []ProjectedRow,
    total: u32 = 0,
    source: SidecarScanSource,
    sidecar_selection: lake_sidecar_selection.Summary = .{},

    pub fn deinit(self: *SidecarScanResult, alloc: Allocator) void {
        for (self.rows) |*row| row.deinit(alloc);
        alloc.free(self.rows);
        self.* = undefined;
    }
};

pub fn executeGroupByAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    request: GroupByRequest,
    materialized: ?*const algebraic_segment.Reader,
) !GroupByResult {
    try validateRequest(request);
    if (materialized != null and request.materialized_source == null) {
        return error.LakeRowsMaterializedSourceRequired;
    }
    if (request.materialized_source) |required| {
        if (required.kind != source.kind) return error.LakeRowsMaterializedSourceMismatch;
    }
    if (materialized) |reader| {
        if (request.deleted_row_refs.len == 0 and request.deleted_row_filter == null and materializedMatches(reader.*, request)) {
            return try resultFromAlgebraicAlloc(alloc, reader.*);
        }
    }
    return try resultFromSourceAlloc(alloc, source, request);
}

pub fn executeExpressionAggregatesAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    request: ExpressionAggregateRequest,
    materialized: ?*const algebraic_segment.ExpressionReader,
) !ExpressionAggregateResult {
    try validateExpressionAggregateRequest(request);
    if (materialized != null and request.materialized_source == null) {
        return error.LakeRowsMaterializedSourceRequired;
    }
    if (request.materialized_source) |required| {
        if (required.kind != source.kind) return error.LakeRowsMaterializedSourceMismatch;
    }
    if (materialized) |reader| {
        if (request.deleted_row_refs.len == 0 and request.deleted_row_filter == null and materializedExpressionMatches(reader.*, request)) {
            return try expressionResultFromAlgebraicAlloc(alloc, reader.*, request.expressions);
        }
    }
    return try expressionResultFromSourceAlloc(alloc, source, request);
}

pub fn scanRowsAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    request: ScanRequest,
) !ScanResult {
    try validateScanRequest(request);

    var deleted_lookup = try RowRefLookup.init(alloc, request.deleted_row_refs, request.limits.max_lookup_row_refs);
    defer deleted_lookup.deinit(alloc);

    var rows = std.ArrayListUnmanaged(ProjectedRow).empty;
    errdefer {
        for (rows.items) |*row| row.deinit(alloc);
        rows.deinit(alloc);
    }

    if (request.limit != null and request.limit.? == 0) {
        return .{ .rows = try rows.toOwnedSlice(alloc), .total = 0 };
    }

    var total: u32 = 0;
    var batches_seen: usize = 0;
    var rows_examined: u64 = 0;
    var materialized_bytes: usize = 0;
    while (try source.next(alloc)) |batch| {
        batches_seen = std.math.add(usize, batches_seen, 1) catch return error.LakeRowsScanBudgetExceeded;
        if (batches_seen > request.limits.max_batches) return error.LakeRowsScanBudgetExceeded;
        const predicate_column = if (request.predicate) |predicate|
            batch.findColumn(predicate.column) orelse return error.RowSourceColumnNotFound
        else
            null;

        for (0..batch.rowCount()) |row_idx| {
            rows_examined = std.math.add(u64, rows_examined, 1) catch return error.LakeRowsScanBudgetExceeded;
            if (rows_examined > request.limits.max_rows_examined) return error.LakeRowsScanBudgetExceeded;
            if (rowIsDeletedLookup(deleted_lookup, request.deleted_row_filter, batch.row_refs[row_idx])) continue;
            if (request.predicate) |predicate| {
                if (!try predicateMatches(predicate, predicate_column.?, row_idx)) continue;
            }
            total = std.math.add(u32, total, 1) catch return error.LakeRowsScanBudgetExceeded;
            if (total > request.limits.max_matching_rows) return error.LakeRowsScanBudgetExceeded;
            if (!limitReached(rows.items.len, request.limit)) {
                if (rows.items.len >= request.limits.max_materialized_rows) return error.LakeRowsScanBudgetExceeded;
                const row_bytes = try projectedRowAllocatedBytes(batch, row_idx, request.projected_columns);
                materialized_bytes = std.math.add(usize, materialized_bytes, row_bytes) catch
                    return error.LakeRowsScanBudgetExceeded;
                if (materialized_bytes > request.limits.max_materialized_bytes) return error.LakeRowsScanBudgetExceeded;
                var projected = try projectRowAlloc(alloc, batch, row_idx, request.projected_columns);
                errdefer projected.deinit(alloc);
                std.debug.assert(projected.allocatedBytes() == row_bytes);
                try rows.append(alloc, projected);
            }
        }
    }

    return .{ .rows = try rows.toOwnedSlice(alloc), .total = total };
}

pub fn scanRowsWithSidecarsAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    request: SidecarScanRequest,
) !SidecarScanResult {
    try validateScanRequest(request.scan);
    try validateSidecarCandidateSets(request.candidates);

    var selection = try lake_sidecar_selection.planAlloc(
        alloc,
        request.base_source,
        request.sidecars,
        request.desired_sidecars,
        request.sidecar_policy,
    );
    defer selection.deinit(alloc);
    const summary: lake_sidecar_selection.Summary = .{
        .selected_count = selection.selected_count,
        .stale_ignored_count = selection.stale_ignored_count,
        .not_requested_count = selection.not_requested_count,
    };

    if (try usableCandidateRefsAlloc(
        alloc,
        request.sidecars,
        selection,
        request.candidates,
        request.scan.limits.max_lookup_row_refs,
    )) |usable| {
        defer alloc.free(usable.row_refs);
        var hydrated = try hydrateRowsForBindingForScanAlloc(
            alloc,
            source,
            usable.binding,
            usable.row_refs,
            request.scan,
        );
        errdefer hydrated.deinit(alloc);
        return .{
            .rows = hydrated.rows,
            .total = hydrated.total,
            .source = .sidecar_hydration,
            .sidecar_selection = summary,
        };
    }

    if (request.sidecar_policy.require_requested and request.desired_sidecars.len != 0) {
        return error.MissingRequiredLakeSidecar;
    }

    var scanned = try scanRowsAlloc(alloc, source, request.scan);
    errdefer scanned.deinit(alloc);
    return .{
        .rows = scanned.rows,
        .total = scanned.total,
        .source = .rowsource_scan,
        .sidecar_selection = summary,
    };
}

pub fn desiredSidecarsFromCandidateSetsAlloc(
    alloc: Allocator,
    candidates: []const SidecarCandidateSet,
) ![]lake_sidecar_selection.DesiredSidecar {
    try validateSidecarCandidateSets(candidates);

    var desired = std.ArrayListUnmanaged(lake_sidecar_selection.DesiredSidecar).empty;
    errdefer desired.deinit(alloc);
    for (candidates) |candidate| {
        if (desiredContainsName(desired.items, candidate.sidecar_name)) continue;
        try desired.append(alloc, .{ .name = candidate.sidecar_name });
    }
    return try desired.toOwnedSlice(alloc);
}

pub fn scanRowsWithAutomaticSidecarsAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    request: SidecarScanRequest,
) !SidecarScanResult {
    if (request.desired_sidecars.len != 0) {
        return try scanRowsWithSidecarsAlloc(alloc, source, request);
    }
    if (request.candidates.len == 0) {
        var scanned = try scanRowsAlloc(alloc, source, request.scan);
        errdefer scanned.deinit(alloc);
        return .{
            .rows = scanned.rows,
            .total = scanned.total,
            .source = .rowsource_scan,
            .sidecar_selection = .{},
        };
    }

    const desired = try desiredSidecarsFromCandidateSetsAlloc(alloc, request.candidates);
    defer alloc.free(desired);
    var automatic_request = request;
    automatic_request.desired_sidecars = desired;
    return try scanRowsWithSidecarsAlloc(alloc, source, automatic_request);
}

pub fn hydrateRowsAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    wanted_refs: []const rowsource.RowRef,
    projected_columns: []const []const u8,
) !HydrateResult {
    return try hydrateRowsWithLimitsAlloc(alloc, source, wanted_refs, projected_columns, .{});
}

pub fn hydrateRowsWithLimitsAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    wanted_refs: []const rowsource.RowRef,
    projected_columns: []const []const u8,
    limits: ScanLimits,
) !HydrateResult {
    return try hydrateRowsInternalAlloc(alloc, source, null, wanted_refs, &.{}, null, projected_columns, null, null, limits, false);
}

pub fn hydrateRowsExcludingDeletedAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    wanted_refs: []const rowsource.RowRef,
    deleted_row_refs: []const rowsource.RowRef,
    projected_columns: []const []const u8,
) !HydrateResult {
    return try hydrateRowsExcludingDeletedWithLimitsAlloc(alloc, source, wanted_refs, deleted_row_refs, projected_columns, .{});
}

pub fn hydrateRowsExcludingDeletedWithLimitsAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    wanted_refs: []const rowsource.RowRef,
    deleted_row_refs: []const rowsource.RowRef,
    projected_columns: []const []const u8,
    limits: ScanLimits,
) !HydrateResult {
    return try hydrateRowsInternalAlloc(alloc, source, null, wanted_refs, deleted_row_refs, null, projected_columns, null, null, limits, false);
}

pub fn hydrateRowsForBindingAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    wanted_refs: []const rowsource.RowRef,
    projected_columns: []const []const u8,
) !HydrateResult {
    try source_binding.validateCandidateRowRefsAgainstBinding(binding, wanted_refs);
    return try hydrateRowsInternalAlloc(alloc, source, binding, wanted_refs, &.{}, null, projected_columns, null, null, .{}, false);
}

pub fn hydrateRowsForBindingExcludingDeletedAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    wanted_refs: []const rowsource.RowRef,
    deleted_row_refs: []const rowsource.RowRef,
    projected_columns: []const []const u8,
) !HydrateResult {
    return try hydrateRowsForBindingExcludingDeletedWithFilterAlloc(
        alloc,
        source,
        binding,
        wanted_refs,
        deleted_row_refs,
        null,
        projected_columns,
    );
}

pub fn hydrateRowsForBindingExcludingDeletedWithFilterAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    wanted_refs: []const rowsource.RowRef,
    deleted_row_refs: []const rowsource.RowRef,
    deleted_row_filter: ?DeletedRowFilter,
    projected_columns: []const []const u8,
) !HydrateResult {
    return try hydrateRowsForBindingExcludingDeletedWithFilterAndLimitsAlloc(
        alloc,
        source,
        binding,
        wanted_refs,
        deleted_row_refs,
        deleted_row_filter,
        projected_columns,
        .{},
    );
}

pub fn hydrateRowsForBindingExcludingDeletedWithFilterAndLimitsAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    wanted_refs: []const rowsource.RowRef,
    deleted_row_refs: []const rowsource.RowRef,
    deleted_row_filter: ?DeletedRowFilter,
    projected_columns: []const []const u8,
    limits: ScanLimits,
) !HydrateResult {
    try source_binding.validateCandidateRowRefsAgainstBinding(binding, wanted_refs);
    return try hydrateRowsInternalAlloc(
        alloc,
        source,
        binding,
        wanted_refs,
        deleted_row_refs,
        deleted_row_filter,
        projected_columns,
        null,
        null,
        limits,
        false,
    );
}

fn hydrateRowsForBindingForScanAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    wanted_refs: []const rowsource.RowRef,
    request: ScanRequest,
) !HydrateResult {
    try source_binding.validateCandidateRowRefsAgainstBinding(binding, wanted_refs);
    return try hydrateRowsInternalAlloc(
        alloc,
        source,
        binding,
        wanted_refs,
        request.deleted_row_refs,
        request.deleted_row_filter,
        request.projected_columns,
        request.predicate,
        request.limit,
        request.limits,
        true,
    );
}

fn hydrateRowsInternalAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: ?source_binding.Binding,
    wanted_refs: []const rowsource.RowRef,
    deleted_row_refs: []const rowsource.RowRef,
    deleted_row_filter: ?DeletedRowFilter,
    projected_columns: []const []const u8,
    predicate: ?Predicate,
    limit: ?usize,
    limits: ScanLimits,
    require_all_wanted: bool,
) !HydrateResult {
    try limits.validate();
    if (wanted_refs.len == 0) return .{ .rows = try alloc.alloc(ProjectedRow, 0), .total = 0 };
    if (projected_columns.len == 0) return error.InvalidLakeRowsQuery;
    if (wanted_refs.len > limits.max_lookup_row_refs) {
        return error.LakeRowsHydrationBudgetExceeded;
    }
    if (deleted_row_refs.len > limits.max_lookup_row_refs) return error.LakeRowsHydrationBudgetExceeded;

    var wanted = try RowRefMatchMap.init(alloc, wanted_refs, limits.max_lookup_row_refs);
    defer wanted.deinit(alloc);
    var deleted_lookup = try RowRefLookup.init(alloc, deleted_row_refs, limits.max_lookup_row_refs);
    defer deleted_lookup.deinit(alloc);
    var wanted_live_count: usize = 0;
    var wanted_it = wanted.map.iterator();
    while (wanted_it.next()) |entry| {
        if (!rowIsDeletedLookup(deleted_lookup, deleted_row_filter, entry.key_ptr.*)) wanted_live_count += 1;
    }

    var rows = std.ArrayListUnmanaged(ProjectedRow).empty;
    errdefer {
        for (rows.items) |*row| row.deinit(alloc);
        rows.deinit(alloc);
    }

    var batches_seen: usize = 0;
    var rows_examined: u64 = 0;
    var materialized_bytes: usize = 0;
    var matching_count: u32 = 0;
    var found_live_count: usize = 0;
    while (try source.next(alloc)) |batch| {
        batches_seen = std.math.add(usize, batches_seen, 1) catch return error.LakeRowsHydrationBudgetExceeded;
        if (batches_seen > limits.max_batches) return error.LakeRowsHydrationBudgetExceeded;
        if (binding) |sidecar_binding| {
            try source_binding.validateBatchSnapshotAgainstBinding(sidecar_binding, batch);
        }
        const predicate_column = if (predicate) |value|
            batch.findColumn(value.column) orelse return error.RowSourceColumnNotFound
        else
            null;
        for (batch.row_refs, 0..) |row_ref, row_idx| {
            rows_examined = std.math.add(u64, rows_examined, 1) catch return error.LakeRowsHydrationBudgetExceeded;
            if (rows_examined > limits.max_rows_examined) return error.LakeRowsHydrationBudgetExceeded;
            if (rowIsDeletedLookup(deleted_lookup, deleted_row_filter, row_ref)) continue;
            const matched = wanted.map.getPtr(row_ref) orelse continue;
            if (matched.*) continue;
            matched.* = true;
            found_live_count += 1;
            if (predicate) |value| {
                if (!try predicateMatches(value, predicate_column.?, row_idx)) continue;
            }
            matching_count = std.math.add(u32, matching_count, 1) catch return error.LakeRowsHydrationBudgetExceeded;
            if (matching_count > limits.max_matching_rows) return error.LakeRowsHydrationBudgetExceeded;
            if (limitReached(rows.items.len, limit)) continue;
            if (rows.items.len >= limits.max_materialized_rows) return error.LakeRowsHydrationBudgetExceeded;
            const row_bytes = try projectedRowAllocatedBytes(batch, row_idx, projected_columns);
            materialized_bytes = std.math.add(usize, materialized_bytes, row_bytes) catch
                return error.LakeRowsHydrationBudgetExceeded;
            if (materialized_bytes > limits.max_materialized_bytes) return error.LakeRowsHydrationBudgetExceeded;
            var projected = try projectRowAlloc(alloc, batch, row_idx, projected_columns);
            errdefer projected.deinit(alloc);
            std.debug.assert(projected.allocatedBytes() == row_bytes);
            try rows.append(alloc, projected);
            if (found_live_count == wanted_live_count) break;
        }
        if (found_live_count == wanted_live_count) break;
    }

    if (require_all_wanted and found_live_count != wanted_live_count) {
        return error.LakeSidecarCandidateHydrationMismatch;
    }
    return .{ .rows = try rows.toOwnedSlice(alloc), .total = matching_count };
}

const UsableCandidateRefs = struct {
    binding: source_binding.Binding,
    row_refs: []rowsource.RowRef,
};

fn validateRequest(request: GroupByRequest) !void {
    try request.limits.validate();
    if (request.deleted_row_refs.len > request.limits.max_deleted_row_refs) return error.LakeRowsAggregateBudgetExceeded;
    if (request.group_column.len == 0) return error.InvalidLakeRowsQuery;
    if (request.op != .count and request.value_column.len == 0) return error.InvalidLakeRowsQuery;
    if (request.materialized_source) |required| try required.validate();
}

fn validateExpressionAggregateRequest(request: ExpressionAggregateRequest) !void {
    try request.limits.validate();
    if (request.deleted_row_refs.len > request.limits.max_deleted_row_refs) return error.LakeRowsAggregateBudgetExceeded;
    if (request.expressions.len == 0) return error.InvalidLakeRowsQuery;
    if (request.expressions.len > request.limits.max_expressions) return error.LakeRowsAggregateBudgetExceeded;
    if (request.materialized_source) |required| try required.validate();
    for (request.expressions, 0..) |expression, idx| {
        if (expression.name.len == 0) return error.InvalidLakeRowsQuery;
        if (expression.op != .count and expression.value_column.len == 0) return error.InvalidLakeRowsQuery;
        for (request.expressions[0..idx]) |previous| {
            if (std.mem.eql(u8, previous.name, expression.name)) return error.InvalidLakeRowsQuery;
        }
    }
}

fn validateScanRequest(request: ScanRequest) !void {
    try request.limits.validate();
    if (request.deleted_row_refs.len > request.limits.max_lookup_row_refs) return error.LakeRowsScanBudgetExceeded;
    if (request.projected_columns.len == 0) return error.InvalidLakeRowsQuery;
    for (request.projected_columns) |column| {
        if (column.len == 0) return error.InvalidLakeRowsQuery;
    }
    if (request.predicate) |predicate| {
        if (predicate.column.len == 0) return error.InvalidLakeRowsQuery;
    }
    if (request.limit) |limit| {
        if (limit > request.limits.max_materialized_rows) return error.InvalidLakeRowsScanLimits;
    }
}

fn limitReached(row_count: usize, limit: ?usize) bool {
    return limit != null and row_count >= limit.?;
}

fn predicateMatches(predicate: Predicate, column: rowsource.ColumnVector, row_idx: usize) !bool {
    if (column.nulls.isNull(row_idx)) return false;
    return switch (predicate.op) {
        .eq_bytes => switch (column.values) {
            .bytes => |items| std.mem.eql(u8, items[row_idx], predicate.bytes_value),
            .json => |items| std.mem.eql(u8, items[row_idx], predicate.bytes_value),
            else => error.UnsupportedLakeRowsPredicateColumnKind,
        },
        .eq_i64 => switch (column.values) {
            .i64 => |items| items[row_idx] == predicate.i64_value,
            .f64 => |items| items[row_idx] == @as(f64, @floatFromInt(predicate.i64_value)),
            else => error.UnsupportedLakeRowsPredicateColumnKind,
        },
        .eq_f64 => switch (column.values) {
            .f64 => |items| items[row_idx] == predicate.f64_value,
            .i64 => |items| if (exactI64FromF64(predicate.f64_value)) |value| items[row_idx] == value else false,
            else => error.UnsupportedLakeRowsPredicateColumnKind,
        },
        .eq_bool => switch (column.values) {
            .bool => |items| items[row_idx] == predicate.bool_value,
            else => error.UnsupportedLakeRowsPredicateColumnKind,
        },
    };
}

fn exactI64FromF64(value: f64) ?i64 {
    if (!std.math.isFinite(value)) return null;
    if (@trunc(value) != value) return null;
    if (value < @as(f64, @floatFromInt(std.math.minInt(i64)))) return null;
    if (value > @as(f64, @floatFromInt(std.math.maxInt(i64)))) return null;
    const as_i64: i64 = @intFromFloat(value);
    if (@as(f64, @floatFromInt(as_i64)) != value) return null;
    return as_i64;
}

fn validateSidecarCandidateSets(candidates: []const SidecarCandidateSet) !void {
    for (candidates) |candidate| {
        if (candidate.sidecar_name.len == 0) return error.InvalidLakeSidecarSelection;
    }
}

fn desiredContainsName(desired: []const lake_sidecar_selection.DesiredSidecar, name: []const u8) bool {
    for (desired) |want| {
        if (std.mem.eql(u8, want.name, name)) return true;
    }
    return false;
}

fn usableCandidateRefsAlloc(
    alloc: Allocator,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    selection: lake_sidecar_selection.Plan,
    candidates: []const SidecarCandidateSet,
    max_refs: usize,
) !?UsableCandidateRefs {
    var refs = std.ArrayListUnmanaged(rowsource.RowRef).empty;
    errdefer refs.deinit(alloc);
    var binding: ?source_binding.Binding = null;
    var found_candidate_set = false;
    var seen = RowRefSetMap.empty;
    defer seen.deinit(alloc);

    for (selection.decisions) |decision| {
        if (decision.action != .use) continue;
        const candidate_set = findCandidateSet(candidates, decision.name) orelse continue;
        const declaration = findSidecarDeclaration(declarations, decision.name) orelse return error.InvalidLakeSidecarSelection;
        try source_binding.validateCandidateRowRefsAgainstBinding(declaration.binding, candidate_set.row_refs);
        if (candidate_set.row_refs.len > max_refs) return error.LakeRowsScanBudgetExceeded;
        if (!found_candidate_set) {
            binding = declaration.binding;
            try seen.ensureTotalCapacity(alloc, @intCast(candidate_set.row_refs.len));
            for (candidate_set.row_refs) |row_ref| {
                const entry = seen.getOrPutAssumeCapacity(row_ref);
                if (!entry.found_existing) try refs.append(alloc, row_ref);
            }
            found_candidate_set = true;
            continue;
        }

        var candidate_lookup = try RowRefLookup.init(alloc, candidate_set.row_refs, max_refs);
        defer candidate_lookup.deinit(alloc);
        var out_idx: usize = 0;
        for (refs.items) |row_ref| {
            if (candidate_lookup.contains(row_ref)) {
                refs.items[out_idx] = row_ref;
                out_idx += 1;
            }
        }
        refs.shrinkRetainingCapacity(out_idx);
    }
    if (!found_candidate_set) return null;
    return .{
        .binding = binding.?,
        .row_refs = try refs.toOwnedSlice(alloc),
    };
}

fn findCandidateSet(
    candidates: []const SidecarCandidateSet,
    sidecar_name: []const u8,
) ?SidecarCandidateSet {
    for (candidates) |candidate| {
        if (std.mem.eql(u8, candidate.sidecar_name, sidecar_name)) return candidate;
    }
    return null;
}

fn findSidecarDeclaration(
    declarations: []const sidecar_manifest.DeclaredArtifact,
    sidecar_name: []const u8,
) ?sidecar_manifest.DeclaredArtifact {
    for (declarations) |declaration| {
        if (std.mem.eql(u8, declaration.name, sidecar_name)) return declaration;
    }
    return null;
}

fn materializedMatches(reader: algebraic_segment.Reader, request: GroupByRequest) bool {
    if (request.materialized_source) |required| {
        if (!materializedSourceMatches(reader.segment.source, required)) return false;
    }
    return std.mem.eql(u8, reader.segment.aggregate.group_column, request.group_column) and
        std.mem.eql(u8, reader.segment.aggregate.value_column, request.value_column) and
        reader.segment.aggregate.op == request.op;
}

fn materializedExpressionMatches(reader: algebraic_segment.ExpressionReader, request: ExpressionAggregateRequest) bool {
    if (request.materialized_source) |required| {
        if (!materializedSourceMatches(reader.materialization.source, required)) return false;
    }
    for (request.expressions) |spec| {
        const expression = findMaterializedExpression(reader.materialization, spec.name) orelse return false;
        if (expression.op != spec.op) return false;
        if (!std.mem.eql(u8, expression.value_column, spec.value_column)) return false;
    }
    return true;
}

fn findMaterializedExpression(
    materialization: algebraic_segment.ExpressionMaterialization,
    name: []const u8,
) ?algebraic_segment.ExpressionFold {
    for (materialization.expressions) |expression| {
        if (std.mem.eql(u8, expression.name, name)) return expression;
    }
    return null;
}

fn materializedSourceMatches(source: algebraic_segment.SourceRef, required: MaterializedSourceRef) bool {
    return source.kind == algebraicSourceKindForRowSourceKind(required.kind) and
        std.mem.eql(u8, source.source_id, required.source_id) and
        std.mem.eql(u8, source.snapshot_id, required.snapshot_id) and
        std.mem.eql(u8, source.schema_fingerprint, required.schema_fingerprint);
}

fn algebraicSourceKindForRowSourceKind(kind: rowsource.SourceKind) algebraic_segment.SourceKind {
    return switch (kind) {
        .relational_store => .relational_store,
        .json_materialized => .relational_store,
        .serverless_fragment => .serverless_fragment,
        .external_parquet => .external_parquet,
        .external_iceberg => .external_iceberg,
        .external_lance => .external_lance,
    };
}

fn resultFromAlgebraicAlloc(
    alloc: Allocator,
    reader: algebraic_segment.Reader,
) !GroupByResult {
    const groups = try alloc.alloc(GroupResult, reader.segment.aggregate.groups.len);
    errdefer alloc.free(groups);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |*group| group.deinit(alloc);
    }

    for (reader.segment.aggregate.groups, groups) |group, *out| {
        out.* = .{
            .key = try alloc.dupe(u8, group.key),
            .value = group.value,
        };
        initialized += 1;
    }

    return .{ .groups = groups, .source = .algebraic_segment };
}

fn expressionResultFromAlgebraicAlloc(
    alloc: Allocator,
    reader: algebraic_segment.ExpressionReader,
    expressions: []const algebraic_segment.ExpressionSpec,
) !ExpressionAggregateResult {
    const out = try alloc.alloc(ExpressionResult, expressions.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*expression| expression.deinit(alloc);
    }

    for (expressions, out) |spec, *item| {
        const expression = findMaterializedExpression(reader.materialization, spec.name) orelse return error.LakeRowsMaterializedExpressionMismatch;
        item.* = .{
            .name = try alloc.dupe(u8, expression.name),
            .value = expression.value,
        };
        initialized += 1;
    }

    return .{ .expressions = out, .source = .algebraic_expression };
}

fn resultFromSourceAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    request: GroupByRequest,
) !GroupByResult {
    var budget = AggregateBudget{ .limits = request.limits };
    var deleted_lookup = try RowRefLookup.init(alloc, request.deleted_row_refs, request.limits.max_deleted_row_refs);
    defer deleted_lookup.deinit(alloc);
    var map = std.StringHashMapUnmanaged(algebraic_segment.AggregateValue).empty;
    defer map.deinit(alloc);
    defer {
        var key_it = map.keyIterator();
        while (key_it.next()) |key| alloc.free(key.*);
    }

    while (try source.next(alloc)) |batch| {
        try budget.admitBatch(batch.rowCount());
        const group_column = batch.findColumn(request.group_column) orelse return error.RowSourceColumnNotFound;
        if (group_column.kind() != .bytes) return error.UnsupportedLakeRowsGroupColumnKind;
        const value_column = if (request.op == .count) null else batch.findColumn(request.value_column) orelse return error.RowSourceColumnNotFound;
        if (value_column) |column| {
            if (column.kind() != .i64) return error.UnsupportedLakeRowsValueColumnKind;
        }

        for (0..batch.rowCount()) |row_idx| {
            if (rowIsDeletedLookup(deleted_lookup, request.deleted_row_filter, batch.row_refs[row_idx])) continue;
            if (group_column.nulls.isNull(row_idx)) continue;
            const key = group_column.values.bytes[row_idx];
            if (key.len == 0) continue;
            const next_value = rowAggregateValue(request.op, value_column, row_idx) orelse continue;
            if (map.getPtr(key)) |value| {
                value.* = try aggregate_math.combine(value.*, next_value);
            } else {
                try budget.admitGroup(map.count(), key.len);
                const owned_key = try alloc.dupe(u8, key);
                errdefer alloc.free(owned_key);
                try map.putNoClobber(alloc, owned_key, next_value);
            }
        }
    }

    const groups = try alloc.alloc(GroupResult, map.count());
    errdefer alloc.free(groups);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |*group| group.deinit(alloc);
    }

    var it = map.iterator();
    while (it.next()) |entry| {
        groups[initialized] = .{
            .key = try alloc.dupe(u8, entry.key_ptr.*),
            .value = entry.value_ptr.*,
        };
        initialized += 1;
    }
    std.mem.sort(GroupResult, groups, {}, compareGroupResult);
    return .{ .groups = groups, .source = .rowsource_scan };
}

const ExpressionAccumulator = struct {
    spec: algebraic_segment.ExpressionSpec,
    value: algebraic_segment.AggregateValue = .{ .count = 0 },
    initialized: bool = false,
};

fn expressionResultFromSourceAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    request: ExpressionAggregateRequest,
) !ExpressionAggregateResult {
    var budget = AggregateBudget{ .limits = request.limits };
    var deleted_lookup = try RowRefLookup.init(alloc, request.deleted_row_refs, request.limits.max_deleted_row_refs);
    defer deleted_lookup.deinit(alloc);
    const accumulators = try alloc.alloc(ExpressionAccumulator, request.expressions.len);
    defer alloc.free(accumulators);
    const value_columns = try alloc.alloc(?rowsource.ColumnVector, request.expressions.len);
    defer alloc.free(value_columns);
    for (request.expressions, accumulators) |spec, *accumulator| {
        accumulator.* = .{ .spec = spec };
    }

    while (try source.next(alloc)) |batch| {
        try budget.admitBatch(batch.rowCount());
        for (accumulators, value_columns) |accumulator, *value_column| {
            value_column.* = if (accumulator.spec.op == .count)
                null
            else
                batch.findColumn(accumulator.spec.value_column) orelse return error.RowSourceColumnNotFound;
            if (value_column.*) |column| {
                if (column.kind() != .i64) return error.UnsupportedLakeRowsValueColumnKind;
            }
        }
        for (0..batch.rowCount()) |row_idx| {
            if (rowIsDeletedLookup(deleted_lookup, request.deleted_row_filter, batch.row_refs[row_idx])) continue;
            for (accumulators, value_columns) |*accumulator, value_column| {
                const next_value = rowAggregateValue(accumulator.spec.op, value_column, row_idx) orelse continue;
                if (!accumulator.initialized) {
                    accumulator.value = next_value;
                    accumulator.initialized = true;
                } else {
                    accumulator.value = try aggregate_math.combine(accumulator.value, next_value);
                }
            }
        }
    }

    const out = try alloc.alloc(ExpressionResult, accumulators.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*expression| expression.deinit(alloc);
    }
    for (accumulators, out) |accumulator, *item| {
        const value = if (accumulator.initialized) accumulator.value else emptyExpressionAggregateValue(accumulator.spec.op) orelse return error.EmptyLakeRowsExpressionAggregate;
        item.* = .{
            .name = try alloc.dupe(u8, accumulator.spec.name),
            .value = value,
        };
        initialized += 1;
    }

    return .{ .expressions = out, .source = .rowsource_scan };
}

fn emptyExpressionAggregateValue(op: algebraic_segment.AggregateOp) ?algebraic_segment.AggregateValue {
    return switch (op) {
        .count => .{ .count = 0 },
        .sum_i64 => .{ .sum_i64 = 0 },
        .min_i64, .max_i64, .avg_i64 => null,
    };
}

fn rowAggregateValue(
    op: algebraic_segment.AggregateOp,
    value_column: ?rowsource.ColumnVector,
    row_idx: usize,
) ?algebraic_segment.AggregateValue {
    return switch (op) {
        .count => .{ .count = 1 },
        .sum_i64 => .{ .sum_i64 = if (value_column.?.nulls.isNull(row_idx)) 0 else value_column.?.values.i64[row_idx] },
        .min_i64 => if (value_column.?.nulls.isNull(row_idx)) null else .{ .min_i64 = value_column.?.values.i64[row_idx] },
        .max_i64 => if (value_column.?.nulls.isNull(row_idx)) null else .{ .max_i64 = value_column.?.values.i64[row_idx] },
        .avg_i64 => if (value_column.?.nulls.isNull(row_idx)) null else .{ .avg_i64 = .{
            .sum_i64 = value_column.?.values.i64[row_idx],
            .count = 1,
        } },
    };
}

fn compareGroupResult(_: void, lhs: GroupResult, rhs: GroupResult) bool {
    return std.mem.lessThan(u8, lhs.key, rhs.key);
}

fn projectRowAlloc(
    alloc: Allocator,
    batch: rowsource.ColumnBatch,
    row_idx: usize,
    projected_columns: []const []const u8,
) !ProjectedRow {
    const row_ref = try source_binding.cloneRowRefAlloc(alloc, batch.row_refs[row_idx]);
    errdefer source_binding.freeOwnedRowRef(alloc, row_ref);
    const cells = try alloc.alloc(ProjectedCell, projected_columns.len);
    errdefer alloc.free(cells);
    var initialized: usize = 0;
    errdefer {
        for (cells[0..initialized]) |*cell| cell.deinit(alloc);
    }

    for (projected_columns, cells) |name, *out| {
        const column = batch.findColumn(name) orelse return error.RowSourceColumnNotFound;
        out.* = .{
            .name = try alloc.dupe(u8, name),
            .value = if (column.nulls.isNull(row_idx)) null else try cloneCellValueAlloc(alloc, column.values, row_idx),
        };
        initialized += 1;
    }

    return .{
        .row_ref = row_ref,
        .cells = cells,
        .owns_row_ref = true,
    };
}

fn projectedRowAllocatedBytes(
    batch: rowsource.ColumnBatch,
    row_idx: usize,
    projected_columns: []const []const u8,
) !usize {
    var total = std.math.add(
        usize,
        @sizeOf(ProjectedRow),
        std.math.mul(usize, projected_columns.len, @sizeOf(ProjectedCell)) catch return std.math.maxInt(usize),
    ) catch return std.math.maxInt(usize);
    total = std.math.add(usize, total, rowRefPayloadBytes(batch.row_refs[row_idx])) catch return std.math.maxInt(usize);
    for (projected_columns) |name| {
        const column = batch.findColumn(name) orelse return error.RowSourceColumnNotFound;
        total = std.math.add(usize, total, name.len) catch return std.math.maxInt(usize);
        if (column.nulls.isNull(row_idx)) continue;
        const payload_bytes = switch (column.values) {
            .bytes, .json => |items| items[row_idx].len,
            .vector_f32 => |items| std.math.mul(usize, items[row_idx].len, @sizeOf(f32)) catch return std.math.maxInt(usize),
            .i64, .f64, .bool => 0,
        };
        total = std.math.add(usize, total, payload_bytes) catch return std.math.maxInt(usize);
    }
    return total;
}

fn rowRefPayloadBytes(row_ref: rowsource.RowRef) usize {
    return switch (row_ref) {
        .relational_key => |key| key.len,
        .serverless => |value| value.fragment_id.len,
        .external => |value| std.math.add(
            usize,
            std.math.add(usize, value.source_id.len, value.snapshot_id.len) catch return std.math.maxInt(usize),
            value.file_id.len,
        ) catch std.math.maxInt(usize),
    };
}

fn cloneCellValueAlloc(
    alloc: Allocator,
    values: rowsource.ColumnValues,
    row_idx: usize,
) !CellValue {
    return switch (values) {
        .bytes => |items| .{ .bytes = try alloc.dupe(u8, items[row_idx]) },
        .json => |items| .{ .json = try alloc.dupe(u8, items[row_idx]) },
        .i64 => |items| .{ .i64 = items[row_idx] },
        .f64 => |items| .{ .f64 = items[row_idx] },
        .bool => |items| .{ .bool = items[row_idx] },
        .vector_f32 => |items| .{ .vector_f32 = try alloc.dupe(f32, items[row_idx]) },
    };
}

const RowRefSetMap = source_binding.RowRefSetMap;

const RowRefMatchSetMap = std.HashMapUnmanaged(
    rowsource.RowRef,
    bool,
    source_binding.RowRefContext,
    std.hash_map.default_max_load_percentage,
);

const RowRefLookup = struct {
    map: RowRefSetMap = .empty,

    fn init(alloc: Allocator, refs: []const rowsource.RowRef, max_refs: usize) !RowRefLookup {
        if (refs.len > max_refs) return error.LakeRowsLookupBudgetExceeded;
        var self = RowRefLookup{};
        errdefer self.deinit(alloc);
        try self.map.ensureTotalCapacity(alloc, @intCast(refs.len));
        for (refs) |row_ref| self.map.putAssumeCapacity(row_ref, {});
        return self;
    }

    fn deinit(self: *RowRefLookup, alloc: Allocator) void {
        self.map.deinit(alloc);
        self.* = undefined;
    }

    fn contains(self: RowRefLookup, row_ref: rowsource.RowRef) bool {
        return self.map.contains(row_ref);
    }
};

const RowRefMatchMap = struct {
    map: RowRefMatchSetMap = .empty,

    fn init(alloc: Allocator, refs: []const rowsource.RowRef, max_refs: usize) !RowRefMatchMap {
        if (refs.len > max_refs) return error.LakeRowsHydrationBudgetExceeded;
        var self = RowRefMatchMap{};
        errdefer self.deinit(alloc);
        try self.map.ensureTotalCapacity(alloc, @intCast(refs.len));
        for (refs) |row_ref| self.map.putAssumeCapacity(row_ref, false);
        return self;
    }

    fn deinit(self: *RowRefMatchMap, alloc: Allocator) void {
        self.map.deinit(alloc);
        self.* = undefined;
    }
};

fn rowIsDeletedLookup(
    deleted_row_refs: RowRefLookup,
    deleted_row_filter: ?DeletedRowFilter,
    row_ref: rowsource.RowRef,
) bool {
    if (deleted_row_refs.contains(row_ref)) return true;
    return if (deleted_row_filter) |filter| filter.contains(row_ref) else false;
}

fn rowRefsEqual(a: rowsource.RowRef, b: rowsource.RowRef) bool {
    return source_binding.rowRefsEqual(a, b);
}

test "lake rows group-by scans RowSource batches" {
    const alloc = std.testing.allocator;
    const local = @import("../../storage/rowsource/local.zig");

    const row_refs = [_]rowsource.RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
        .{ .relational_key = "row:c" },
    };
    const tenants = [_][]const u8{ "t2", "t1", "t2" };
    const amounts = [_]i64{ 7, 11, 13 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "lsm-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try local.relationalStoreSource(&batches);

    var result = try executeGroupByAlloc(alloc, batch_source.rowSource(), .{
        .group_column = "tenant",
        .value_column = "amount",
        .op = .sum_i64,
    }, null);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.groups.len);
    try std.testing.expectEqual(@as(i64, 11), result.find("t1").?.sum_i64);
    try std.testing.expectEqual(@as(i64, 20), result.find("t2").?.sum_i64);
    try std.testing.expectEqual(.rowsource_scan, result.source);
}

test "lake rows aggregates reject overflow and enforce cumulative budgets" {
    const alloc = std.testing.allocator;
    const local = @import("../../storage/rowsource/local.zig");

    const row_refs = [_]rowsource.RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
    };
    const tenants = [_][]const u8{ "t1", "t1" };
    const amounts = [_]i64{ std.math.maxInt(i64), 1 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "lsm-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};

    var overflow_source = try local.relationalStoreSource(&batches);
    try std.testing.expectError(error.AlgebraicAggregateOverflow, executeGroupByAlloc(alloc, overflow_source.rowSource(), .{
        .group_column = "tenant",
        .value_column = "amount",
        .op = .sum_i64,
    }, null));

    var bounded_source = try local.relationalStoreSource(&batches);
    try std.testing.expectError(error.LakeRowsAggregateBudgetExceeded, executeGroupByAlloc(alloc, bounded_source.rowSource(), .{
        .group_column = "tenant",
        .value_column = "amount",
        .op = .sum_i64,
        .limits = .{ .max_rows_examined = 1 },
    }, null));
}

test "lake rows and algebraic sidecars agree that all-null sums are zero" {
    const alloc = std.testing.allocator;
    const local = @import("../../storage/rowsource/local.zig");

    const row_refs = [_]rowsource.RowRef{.{ .relational_key = "row:a" }};
    const tenants = [_][]const u8{"t1"};
    const amounts = [_]i64{999};
    const nulls = [_]u8{1};
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts }, .nulls = .{ .bytes = &nulls } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "lsm-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    };
    const batches = [_]rowsource.ColumnBatch{batch};

    var direct_source = try local.relationalStoreSource(&batches);
    var direct = try executeGroupByAlloc(alloc, direct_source.rowSource(), .{
        .group_column = "tenant",
        .value_column = "amount",
        .op = .sum_i64,
    }, null);
    defer direct.deinit(alloc);

    var sidecar = try algebraic_segment.buildGroupByAggregateAlloc(alloc, batch, .{
        .source_kind = .relational_store,
        .snapshot_id = "lsm-1",
        .schema_fingerprint = "schema-v1",
        .group_column = "tenant",
        .value_column = "amount",
        .op = .sum_i64,
    });
    defer sidecar.deinit(alloc);

    try std.testing.expectEqual(@as(i64, 0), direct.find("t1").?.sum_i64);
    try std.testing.expectEqual(@as(usize, 1), sidecar.aggregate.groups.len);
    try std.testing.expectEqual(@as(i64, 0), sidecar.aggregate.groups[0].value.sum_i64);
}

test "lake rows group-by excludes deleted row refs" {
    const alloc = std.testing.allocator;
    const external = @import("../../storage/rowsource/external.zig");

    const binding = external.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external.makeRowRef(binding, "file-a.parquet", 0, 0),
        try external.makeRowRef(binding, "file-a.parquet", 0, 1),
        try external.makeRowRef(binding, "file-b.parquet", 1, 0),
    };
    const tenants = [_][]const u8{ "t1", "t1", "t2" };
    const amounts = [_]i64{ 10, 20, 30 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = binding.snapshot(),
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try external.BatchSource.init(binding, &batches);

    const deleted = [_]rowsource.RowRef{row_refs[1]};
    var result = try executeGroupByAlloc(alloc, batch_source.rowSource(), .{
        .group_column = "tenant",
        .value_column = "amount",
        .op = .sum_i64,
        .deleted_row_refs = &deleted,
    }, null);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.groups.len);
    try std.testing.expectEqual(@as(i64, 10), result.find("t1").?.sum_i64);
    try std.testing.expectEqual(@as(i64, 30), result.find("t2").?.sum_i64);
    try std.testing.expectEqual(.rowsource_scan, result.source);
}

test "lake rows group-by can use algebraic segment materialization" {
    const alloc = std.testing.allocator;
    var segment = algebraic_segment.Segment{
        .source = .{
            .kind = .serverless_fragment,
            .snapshot_id = try alloc.dupe(u8, "manifest-1"),
            .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
            .source_id = try alloc.dupe(u8, "orders"),
        },
        .aggregate = .{
            .group_column = try alloc.dupe(u8, "tenant"),
            .value_column = try alloc.dupe(u8, "amount"),
            .op = .sum_i64,
            .groups = try alloc.alloc(algebraic_segment.GroupFold, 1),
        },
    };
    defer segment.deinit(alloc);
    segment.aggregate.groups[0] = .{
        .key = try alloc.dupe(u8, "t1"),
        .value = .{ .sum_i64 = 42 },
    };

    const encoded = try algebraic_segment.encodeAlloc(alloc, segment);
    defer alloc.free(encoded);
    var reader = try algebraic_segment.Reader.decodeAlloc(alloc, encoded);
    defer reader.deinit();

    const EmptySource = struct {
        fn next(_: *anyopaque, _: Allocator) !?rowsource.ColumnBatch {
            return null;
        }
    };
    var dummy: u8 = 0;
    const source = rowsource.Source{
        .kind = .serverless_fragment,
        .ctx = &dummy,
        .next_batch = EmptySource.next,
    };

    var result = try executeGroupByAlloc(alloc, source, .{
        .group_column = "tenant",
        .value_column = "amount",
        .op = .sum_i64,
        .materialized_source = .{
            .kind = .serverless_fragment,
            .source_id = "orders",
            .snapshot_id = "manifest-1",
            .schema_fingerprint = "schema-v1",
        },
    }, &reader);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.groups.len);
    try std.testing.expectEqual(@as(i64, 42), result.find("t1").?.sum_i64);
    try std.testing.expectEqual(.algebraic_segment, result.source);
}

test "lake rows group-by requires source contract for algebraic materialization" {
    const alloc = std.testing.allocator;
    var segment = algebraic_segment.Segment{
        .source = .{
            .kind = .serverless_fragment,
            .snapshot_id = try alloc.dupe(u8, "manifest-1"),
            .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
            .source_id = try alloc.dupe(u8, "orders"),
        },
        .aggregate = .{
            .group_column = try alloc.dupe(u8, "tenant"),
            .value_column = try alloc.dupe(u8, "amount"),
            .op = .sum_i64,
            .groups = try alloc.alloc(algebraic_segment.GroupFold, 1),
        },
    };
    defer segment.deinit(alloc);
    segment.aggregate.groups[0] = .{
        .key = try alloc.dupe(u8, "t1"),
        .value = .{ .sum_i64 = 42 },
    };

    const encoded = try algebraic_segment.encodeAlloc(alloc, segment);
    defer alloc.free(encoded);
    var reader = try algebraic_segment.Reader.decodeAlloc(alloc, encoded);
    defer reader.deinit();

    const EmptySource = struct {
        fn next(_: *anyopaque, _: Allocator) !?rowsource.ColumnBatch {
            return null;
        }
    };
    var dummy: u8 = 0;
    const source = rowsource.Source{
        .kind = .serverless_fragment,
        .ctx = &dummy,
        .next_batch = EmptySource.next,
    };

    try std.testing.expectError(error.LakeRowsMaterializedSourceRequired, executeGroupByAlloc(alloc, source, .{
        .group_column = "tenant",
        .value_column = "amount",
        .op = .sum_i64,
    }, &reader));
}

test "lake rows group-by rejects stale algebraic materialization source" {
    const alloc = std.testing.allocator;
    const local = @import("../../storage/rowsource/local.zig");

    var segment = algebraic_segment.Segment{
        .source = .{
            .kind = .relational_store,
            .snapshot_id = try alloc.dupe(u8, "manifest-1"),
            .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
            .source_id = try alloc.dupe(u8, "orders"),
        },
        .aggregate = .{
            .group_column = try alloc.dupe(u8, "tenant"),
            .value_column = try alloc.dupe(u8, "amount"),
            .op = .sum_i64,
            .groups = try alloc.alloc(algebraic_segment.GroupFold, 1),
        },
    };
    defer segment.deinit(alloc);
    segment.aggregate.groups[0] = .{
        .key = try alloc.dupe(u8, "t1"),
        .value = .{ .sum_i64 = 999 },
    };

    const encoded = try algebraic_segment.encodeAlloc(alloc, segment);
    defer alloc.free(encoded);
    var reader = try algebraic_segment.Reader.decodeAlloc(alloc, encoded);
    defer reader.deinit();

    const row_refs = [_]rowsource.RowRef{.{ .relational_key = "row:a" }};
    const tenants = [_][]const u8{"t1"};
    const amounts = [_]i64{10};
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "manifest-2" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try local.relationalStoreSource(&batches);

    var result = try executeGroupByAlloc(alloc, batch_source.rowSource(), .{
        .group_column = "tenant",
        .value_column = "amount",
        .op = .sum_i64,
        .materialized_source = .{
            .kind = .relational_store,
            .source_id = "orders",
            .snapshot_id = "manifest-2",
            .schema_fingerprint = "schema-v1",
        },
    }, &reader);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.groups.len);
    try std.testing.expectEqual(@as(i64, 10), result.find("t1").?.sum_i64);
    try std.testing.expectEqual(.rowsource_scan, result.source);
}

test "lake rows expression aggregates can use algebraic materialization" {
    const alloc = std.testing.allocator;
    var materialization = algebraic_segment.ExpressionMaterialization{
        .source = .{
            .kind = .serverless_fragment,
            .snapshot_id = try alloc.dupe(u8, "manifest-1"),
            .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
            .source_id = try alloc.dupe(u8, "orders"),
        },
        .expressions = try alloc.alloc(algebraic_segment.ExpressionFold, 2),
    };
    defer materialization.deinit(alloc);
    materialization.expressions[0] = .{
        .name = try alloc.dupe(u8, "row_count"),
        .op = .count,
        .value = .{ .count = 3 },
    };
    materialization.expressions[1] = .{
        .name = try alloc.dupe(u8, "amount_sum"),
        .value_column = try alloc.dupe(u8, "amount"),
        .op = .sum_i64,
        .value = .{ .sum_i64 = 42 },
    };

    const encoded = try algebraic_segment.encodeExpressionAlloc(alloc, materialization);
    defer alloc.free(encoded);
    var reader = try algebraic_segment.ExpressionReader.decodeAlloc(alloc, encoded);
    defer reader.deinit();

    const EmptySource = struct {
        fn next(_: *anyopaque, _: Allocator) !?rowsource.ColumnBatch {
            return null;
        }
    };
    var dummy: u8 = 0;
    const source = rowsource.Source{
        .kind = .serverless_fragment,
        .ctx = &dummy,
        .next_batch = EmptySource.next,
    };
    const specs = [_]algebraic_segment.ExpressionSpec{
        .{ .name = "row_count", .op = .count },
        .{ .name = "amount_sum", .value_column = "amount", .op = .sum_i64 },
    };

    var result = try executeExpressionAggregatesAlloc(alloc, source, .{
        .expressions = &specs,
        .materialized_source = .{
            .kind = .serverless_fragment,
            .source_id = "orders",
            .snapshot_id = "manifest-1",
            .schema_fingerprint = "schema-v1",
        },
    }, &reader);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.expressions.len);
    try std.testing.expectEqual(@as(u64, 3), result.find("row_count").?.count);
    try std.testing.expectEqual(@as(i64, 42), result.find("amount_sum").?.sum_i64);
    try std.testing.expectEqual(.algebraic_expression, result.source);
}

test "lake rows expression aggregates require source contract for algebraic materialization" {
    const alloc = std.testing.allocator;
    var materialization = algebraic_segment.ExpressionMaterialization{
        .source = .{
            .kind = .serverless_fragment,
            .snapshot_id = try alloc.dupe(u8, "manifest-1"),
            .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
            .source_id = try alloc.dupe(u8, "orders"),
        },
        .expressions = try alloc.alloc(algebraic_segment.ExpressionFold, 1),
    };
    defer materialization.deinit(alloc);
    materialization.expressions[0] = .{
        .name = try alloc.dupe(u8, "row_count"),
        .op = .count,
        .value = .{ .count = 3 },
    };

    const encoded = try algebraic_segment.encodeExpressionAlloc(alloc, materialization);
    defer alloc.free(encoded);
    var reader = try algebraic_segment.ExpressionReader.decodeAlloc(alloc, encoded);
    defer reader.deinit();

    const EmptySource = struct {
        fn next(_: *anyopaque, _: Allocator) !?rowsource.ColumnBatch {
            return null;
        }
    };
    var dummy: u8 = 0;
    const source = rowsource.Source{
        .kind = .serverless_fragment,
        .ctx = &dummy,
        .next_batch = EmptySource.next,
    };
    const specs = [_]algebraic_segment.ExpressionSpec{.{ .name = "row_count", .op = .count }};

    try std.testing.expectError(error.LakeRowsMaterializedSourceRequired, executeExpressionAggregatesAlloc(alloc, source, .{
        .expressions = &specs,
    }, &reader));
}

test "lake rows expression aggregates reject stale algebraic materialization source" {
    const alloc = std.testing.allocator;
    const local = @import("../../storage/rowsource/local.zig");

    var materialization = algebraic_segment.ExpressionMaterialization{
        .source = .{
            .kind = .relational_store,
            .snapshot_id = try alloc.dupe(u8, "manifest-1"),
            .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
            .source_id = try alloc.dupe(u8, "orders"),
        },
        .expressions = try alloc.alloc(algebraic_segment.ExpressionFold, 1),
    };
    defer materialization.deinit(alloc);
    materialization.expressions[0] = .{
        .name = try alloc.dupe(u8, "amount_sum"),
        .value_column = try alloc.dupe(u8, "amount"),
        .op = .sum_i64,
        .value = .{ .sum_i64 = 999 },
    };

    const encoded = try algebraic_segment.encodeExpressionAlloc(alloc, materialization);
    defer alloc.free(encoded);
    var reader = try algebraic_segment.ExpressionReader.decodeAlloc(alloc, encoded);
    defer reader.deinit();

    const row_refs = [_]rowsource.RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
    };
    const amounts = [_]i64{ 10, 20 };
    const columns = [_]rowsource.ColumnVector{.{ .name = "amount", .values = .{ .i64 = &amounts } }};
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "manifest-2" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try local.relationalStoreSource(&batches);
    const specs = [_]algebraic_segment.ExpressionSpec{.{ .name = "amount_sum", .value_column = "amount", .op = .sum_i64 }};

    var result = try executeExpressionAggregatesAlloc(alloc, batch_source.rowSource(), .{
        .expressions = &specs,
        .materialized_source = .{
            .kind = .relational_store,
            .source_id = "orders",
            .snapshot_id = "manifest-2",
            .schema_fingerprint = "schema-v1",
        },
    }, &reader);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.expressions.len);
    try std.testing.expectEqual(@as(i64, 30), result.find("amount_sum").?.sum_i64);
    try std.testing.expectEqual(.rowsource_scan, result.source);
}

test "lake rows scans projected local rows with a predicate" {
    const alloc = std.testing.allocator;
    const local = @import("../../storage/rowsource/local.zig");

    const row_refs = [_]rowsource.RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
        .{ .relational_key = "row:c" },
    };
    const tenants = [_][]const u8{ "t1", "t2", "t2" };
    const amounts = [_]i64{ 10, 20, 30 };
    const active = [_]bool{ true, true, false };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
        .{ .name = "active", .values = .{ .bool = &active } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "lsm-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try local.relationalStoreSource(&batches);

    const projection = [_][]const u8{ "amount", "active" };
    var result = try scanRowsAlloc(alloc, batch_source.rowSource(), .{
        .projected_columns = &projection,
        .predicate = .{
            .column = "tenant",
            .op = .eq_bytes,
            .bytes_value = "t2",
        },
        .limit = 1,
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expect(rowRefsEqual(row_refs[1], result.rows[0].row_ref));
    try std.testing.expect(result.rows[0].owns_row_ref);
    try std.testing.expect(@intFromPtr(result.rows[0].row_ref.relational_key.ptr) != @intFromPtr(row_refs[1].relational_key.ptr));
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
    try std.testing.expectEqual(true, result.rows[0].find("active").?.value.?.bool);
}

test "lake rows materialize row refs independently of source batch lifetime" {
    const alloc = std.testing.allocator;

    const ReleasingSource = struct {
        key: ?[]u8 = null,
        row_refs: [1]rowsource.RowRef = undefined,
        values: [1]i64 = .{42},
        columns: [1]rowsource.ColumnVector = undefined,
        emitted: bool = false,

        fn next(ptr: *anyopaque, a: Allocator) !?rowsource.ColumnBatch {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.emitted) {
                if (self.key) |key| a.free(key);
                self.key = null;
                return null;
            }
            self.emitted = true;
            self.key = try a.dupe(u8, "transient:row");
            self.row_refs[0] = .{ .relational_key = self.key.? };
            self.columns[0] = .{ .name = "amount", .values = .{ .i64 = &self.values } };
            return .{
                .snapshot = .{ .table_id = "orders", .snapshot_id = "lsm-1" },
                .row_refs = &self.row_refs,
                .columns = &self.columns,
            };
        }

        fn deinit(self: *@This(), a: Allocator) void {
            if (self.key) |key| a.free(key);
            self.key = null;
        }
    };

    var state = ReleasingSource{};
    defer state.deinit(alloc);
    var result = try scanRowsAlloc(alloc, .{
        .kind = .relational_store,
        .ctx = &state,
        .next_batch = ReleasingSource.next,
    }, .{ .projected_columns = &.{"amount"} });
    defer result.deinit(alloc);

    try std.testing.expect(state.key == null);
    try std.testing.expect(result.rows[0].owns_row_ref);
    try std.testing.expectEqualStrings("transient:row", result.rows[0].row_ref.relational_key);
}

test "lake rows numeric predicates match exact i64 and f64 representations" {
    const alloc = std.testing.allocator;
    const local = @import("../../storage/rowsource/local.zig");

    const row_refs = [_]rowsource.RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
        .{ .relational_key = "row:c" },
    };
    const amount_i64 = [_]i64{ 10, 20, 30 };
    const score_f64 = [_]f64{ 1.5, 2.0, 3.25 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amount_i64 } },
        .{ .name = "score", .values = .{ .f64 = &score_f64 } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "lsm-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};

    const i64_projection = [_][]const u8{"amount"};
    var i64_source = try local.relationalStoreSource(&batches);
    var i64_result = try scanRowsAlloc(alloc, i64_source.rowSource(), .{
        .projected_columns = &i64_projection,
        .predicate = .{
            .column = "amount",
            .op = .eq_f64,
            .f64_value = 20.0,
        },
    });
    defer i64_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), i64_result.rows.len);
    try std.testing.expect(rowRefsEqual(row_refs[1], i64_result.rows[0].row_ref));

    const f64_projection = [_][]const u8{"score"};
    var f64_source = try local.relationalStoreSource(&batches);
    var f64_result = try scanRowsAlloc(alloc, f64_source.rowSource(), .{
        .projected_columns = &f64_projection,
        .predicate = .{
            .column = "score",
            .op = .eq_i64,
            .i64_value = 2,
        },
    });
    defer f64_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), f64_result.rows.len);
    try std.testing.expect(rowRefsEqual(row_refs[1], f64_result.rows[0].row_ref));

    var fractional_source = try local.relationalStoreSource(&batches);
    var fractional_result = try scanRowsAlloc(alloc, fractional_source.rowSource(), .{
        .projected_columns = &i64_projection,
        .predicate = .{
            .column = "amount",
            .op = .eq_f64,
            .f64_value = 20.5,
        },
    });
    defer fractional_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), fractional_result.rows.len);
}

test "lake rows scans external rows through the same projection contract" {
    const alloc = std.testing.allocator;
    const external = @import("../../storage/rowsource/external.zig");

    const binding = external.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external.makeRowRef(binding, "file-a.parquet", 0, 0),
        try external.makeRowRef(binding, "file-a.parquet", 0, 1),
        try external.makeRowRef(binding, "file-b.parquet", 1, 0),
    };
    const tenants = [_][]const u8{ "t1", "t2", "t2" };
    const amounts = [_]i64{ 10, 20, 30 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = binding.snapshot(),
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try external.BatchSource.init(binding, &batches);

    const projection = [_][]const u8{"amount"};
    var result = try scanRowsAlloc(alloc, batch_source.rowSource(), .{
        .projected_columns = &projection,
        .predicate = .{
            .column = "tenant",
            .op = .eq_bytes,
            .bytes_value = "t2",
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expect(rowRefsEqual(row_refs[1], result.rows[0].row_ref));
    try std.testing.expect(rowRefsEqual(row_refs[2], result.rows[1].row_ref));
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
    try std.testing.expectEqual(@as(i64, 30), result.rows[1].find("amount").?.value.?.i64);
}

test "lake rows scans exclude deleted row refs before predicates and limits" {
    const alloc = std.testing.allocator;
    const external = @import("../../storage/rowsource/external.zig");

    const binding = external.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external.makeRowRef(binding, "file-a.parquet", 0, 0),
        try external.makeRowRef(binding, "file-a.parquet", 0, 1),
        try external.makeRowRef(binding, "file-b.parquet", 1, 0),
    };
    const tenants = [_][]const u8{ "t2", "t2", "t2" };
    const amounts = [_]i64{ 10, 20, 30 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = binding.snapshot(),
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try external.BatchSource.init(binding, &batches);

    const projection = [_][]const u8{"amount"};
    const deleted = [_]rowsource.RowRef{row_refs[0]};
    var result = try scanRowsAlloc(alloc, batch_source.rowSource(), .{
        .projected_columns = &projection,
        .predicate = .{
            .column = "tenant",
            .op = .eq_bytes,
            .bytes_value = "t2",
        },
        .limit = 1,
        .deleted_row_refs = &deleted,
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 2), result.total);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expect(rowRefsEqual(row_refs[1], result.rows[0].row_ref));
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
}

test "lake rows enforce global scan budgets and callback deletion filters" {
    const alloc = std.testing.allocator;
    const local = @import("../../storage/rowsource/local.zig");

    const row_refs = [_]rowsource.RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
        .{ .relational_key = "row:c" },
    };
    const amounts = [_]i64{ 10, 20, 30 };
    const columns = [_]rowsource.ColumnVector{.{ .name = "amount", .values = .{ .i64 = &amounts } }};
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "1" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    const Filter = struct {
        deleted: rowsource.RowRef,

        fn contains(ctx: *const anyopaque, row_ref: rowsource.RowRef) bool {
            const self: *const @This() = @ptrCast(@alignCast(ctx));
            return rowRefsEqual(self.deleted, row_ref);
        }
    };
    const filter_ctx = Filter{ .deleted = row_refs[1] };
    const projection = [_][]const u8{"amount"};
    var filtered_source = try local.relationalStoreSource(&batches);
    var filtered = try scanRowsAlloc(alloc, filtered_source.rowSource(), .{
        .projected_columns = &projection,
        .deleted_row_filter = .{ .ctx = &filter_ctx, .contains_fn = Filter.contains },
    });
    defer filtered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), filtered.total);
    try std.testing.expectEqual(@as(usize, 2), filtered.rows.len);

    var bounded_source = try local.relationalStoreSource(&batches);
    try std.testing.expectError(error.LakeRowsScanBudgetExceeded, scanRowsAlloc(alloc, bounded_source.rowSource(), .{
        .projected_columns = &projection,
        .limits = .{ .max_rows_examined = 2 },
    }));

    var byte_bounded_source = try local.relationalStoreSource(&batches);
    try std.testing.expectError(error.LakeRowsScanBudgetExceeded, scanRowsAlloc(alloc, byte_bounded_source.rowSource(), .{
        .projected_columns = &projection,
        .limits = .{ .max_materialized_bytes = 1 },
    }));
}

test "lake rows hydrates projected cells by row ref" {
    const alloc = std.testing.allocator;
    const local = @import("../../storage/rowsource/local.zig");

    const row_refs = [_]rowsource.RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
    };
    const amounts = [_]i64{ 10, 20 };
    const attrs = [_][]const u8{ "{\"tier\":\"free\"}", "{\"tier\":\"pro\"}" };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
        .{ .name = "attrs", .values = .{ .json = &attrs } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "lsm-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try local.relationalStoreSource(&batches);

    const wanted = [_]rowsource.RowRef{.{ .relational_key = "row:b" }};
    const projection = [_][]const u8{ "amount", "attrs" };
    var result = try hydrateRowsAlloc(alloc, batch_source.rowSource(), &wanted, &projection);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expect(rowRefsEqual(wanted[0], result.rows[0].row_ref));
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
    try std.testing.expectEqualStrings("{\"tier\":\"pro\"}", result.rows[0].find("attrs").?.value.?.json);
}

test "lake rows hydration deduplicates candidates and enforces scan budgets" {
    const alloc = std.testing.allocator;
    const local = @import("../../storage/rowsource/local.zig");

    const row_refs = [_]rowsource.RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
    };
    const amounts = [_]i64{ 10, 20 };
    const columns = [_]rowsource.ColumnVector{.{ .name = "amount", .values = .{ .i64 = &amounts } }};
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "lsm-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    const wanted = [_]rowsource.RowRef{ row_refs[1], row_refs[1] };
    const projection = [_][]const u8{"amount"};

    var deduplicated_source = try local.relationalStoreSource(&batches);
    var result = try hydrateRowsWithLimitsAlloc(
        alloc,
        deduplicated_source.rowSource(),
        &wanted,
        &projection,
        .{ .max_materialized_rows = 1 },
    );
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), result.total);
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);

    var bounded_source = try local.relationalStoreSource(&batches);
    try std.testing.expectError(error.LakeRowsHydrationBudgetExceeded, hydrateRowsWithLimitsAlloc(
        alloc,
        bounded_source.rowSource(),
        &wanted,
        &projection,
        .{ .max_materialized_rows = 1, .max_rows_examined = 1 },
    ));
}

test "lake rows hydration excludes deleted row refs" {
    const alloc = std.testing.allocator;
    const external = @import("../../storage/rowsource/external.zig");

    const binding = external.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external.makeRowRef(binding, "file-a.parquet", 0, 0),
        try external.makeRowRef(binding, "file-a.parquet", 0, 1),
    };
    const amounts = [_]i64{ 10, 20 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = binding.snapshot(),
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try external.BatchSource.init(binding, &batches);

    const projection = [_][]const u8{"amount"};
    const deleted = [_]rowsource.RowRef{row_refs[0]};
    var result = try hydrateRowsExcludingDeletedAlloc(
        alloc,
        batch_source.rowSource(),
        &row_refs,
        &deleted,
        &projection,
    );
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), result.total);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expect(rowRefsEqual(row_refs[1], result.rows[0].row_ref));
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
}

test "lake rows binding-aware hydration rejects stale external candidates" {
    const alloc = std.testing.allocator;
    const external = @import("../../storage/rowsource/external.zig");

    const external_binding = external.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
    };
    const row_refs = [_]rowsource.RowRef{
        try external.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external.makeRowRef(external_binding, "file-a.parquet", 0, 1),
    };
    const amounts = [_]i64{ 10, 20 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = external_binding.snapshot(),
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try external.BatchSource.init(external_binding, &batches);

    const sidecar_binding = source_binding.Binding{
        .sidecar_kind = .vector,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{"embedding"},
        .index_config_hash = "sha256:vector",
    };
    const projection = [_][]const u8{"amount"};
    var matched = try hydrateRowsForBindingAlloc(
        alloc,
        batch_source.rowSource(),
        sidecar_binding,
        &[_]rowsource.RowRef{row_refs[1]},
        &projection,
    );
    defer matched.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), matched.total);
    try std.testing.expectEqual(@as(usize, 1), matched.rows.len);
    try std.testing.expect(rowRefsEqual(row_refs[1], matched.rows[0].row_ref));
    try std.testing.expectEqual(@as(i64, 20), matched.rows[0].find("amount").?.value.?.i64);

    const stale_refs = [_]rowsource.RowRef{.{ .external = .{
        .source_id = "events",
        .snapshot_id = "iceberg-8",
        .file_id = "file-a.parquet",
        .row_group_ordinal = 0,
        .row_ordinal = 1,
    } }};
    try std.testing.expectError(error.SidecarSourceBindingMismatch, hydrateRowsForBindingAlloc(
        alloc,
        batch_source.rowSource(),
        sidecar_binding,
        &stale_refs,
        &projection,
    ));
}

test "lake rows sidecar scan hydrates selected external candidates" {
    const alloc = std.testing.allocator;
    const external = @import("../../storage/rowsource/external.zig");
    const artifact_ref = @import("../manifest/artifact_ref.zig");

    const external_binding = external.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
    };
    const row_refs = [_]rowsource.RowRef{
        try external.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external.makeRowRef(external_binding, "file-a.parquet", 0, 1),
        try external.makeRowRef(external_binding, "file-b.parquet", 1, 0),
    };
    const amounts = [_]i64{ 10, 20, 30 };
    const tenants = [_][]const u8{ "t1", "t2", "t2" };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = external_binding.snapshot(),
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try external.BatchSource.init(external_binding, &batches);

    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.embedding.vector",
        .binding = .{
            .sidecar_kind = .vector,
            .source_kind = .external_iceberg,
            .row_ref_kind = .external,
            .source_id = "events",
            .snapshot_id = "iceberg-9",
            .schema_fingerprint = "schema-v2",
            .column_bindings = &[_][]const u8{"embedding"},
            .index_config_hash = "sha256:vector",
        },
        .artifact = .{
            .kind = artifact_ref.ArtifactKind.vector_segment,
            .name = "events.embedding.vector",
            .artifact_id = "vector-1",
            .byte_len = 128,
            .checksum = "len:128",
        },
    };
    const candidate_refs = [_]rowsource.RowRef{ row_refs[2], row_refs[1] };
    const candidates = [_]SidecarCandidateSet{.{
        .sidecar_name = "events.embedding.vector",
        .row_refs = &candidate_refs,
    }};
    const projection = [_][]const u8{"amount"};

    var result = try scanRowsWithSidecarsAlloc(alloc, batch_source.rowSource(), .{
        .scan = .{ .projected_columns = &projection },
        .base_source = .{ .external_iceberg = .{
            .format = .iceberg,
            .source_uri = "s3://bucket/warehouse/events",
            .snapshot_id = "iceberg-9",
            .schema_fingerprint = "schema-v2",
        } },
        .sidecars = &[_]sidecar_manifest.DeclaredArtifact{declaration},
        .desired_sidecars = &[_]lake_sidecar_selection.DesiredSidecar{.{ .kind = .vector }},
        .sidecar_policy = .{ .require_requested = true },
        .candidates = &candidates,
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(SidecarScanSource.sidecar_hydration, result.source);
    try std.testing.expectEqual(@as(u32, 1), result.sidecar_selection.selected_count);
    try std.testing.expectEqual(@as(u32, 2), result.total);
    try std.testing.expect(rowRefsEqual(row_refs[1], result.rows[0].row_ref));
    try std.testing.expect(rowRefsEqual(row_refs[2], result.rows[1].row_ref));
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
    try std.testing.expectEqual(@as(i64, 30), result.rows[1].find("amount").?.value.?.i64);

    var filtered_source = try external.BatchSource.init(external_binding, &batches);
    var filtered = try scanRowsWithSidecarsAlloc(alloc, filtered_source.rowSource(), .{
        .scan = .{
            .projected_columns = &projection,
            .predicate = .{ .column = "tenant", .op = .eq_bytes, .bytes_value = "t2" },
            .limit = 1,
        },
        .base_source = .{ .external_iceberg = .{
            .format = .iceberg,
            .source_uri = "s3://bucket/warehouse/events",
            .snapshot_id = "iceberg-9",
            .schema_fingerprint = "schema-v2",
        } },
        .sidecars = &[_]sidecar_manifest.DeclaredArtifact{declaration},
        .desired_sidecars = &[_]lake_sidecar_selection.DesiredSidecar{.{ .kind = .vector }},
        .sidecar_policy = .{ .require_requested = true },
        .candidates = &candidates,
    });
    defer filtered.deinit(alloc);

    try std.testing.expectEqual(SidecarScanSource.sidecar_hydration, filtered.source);
    try std.testing.expectEqual(@as(u32, 2), filtered.total);
    try std.testing.expectEqual(@as(usize, 1), filtered.rows.len);
    try std.testing.expect(rowRefsEqual(row_refs[1], filtered.rows[0].row_ref));
}

test "lake rows sidecar scan excludes deleted candidate refs" {
    const alloc = std.testing.allocator;
    const external = @import("../../storage/rowsource/external.zig");
    const artifact_ref = @import("../manifest/artifact_ref.zig");

    const external_binding = external.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
    };
    const row_refs = [_]rowsource.RowRef{
        try external.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external.makeRowRef(external_binding, "file-a.parquet", 0, 1),
        try external.makeRowRef(external_binding, "file-b.parquet", 1, 0),
    };
    const amounts = [_]i64{ 10, 20, 30 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = external_binding.snapshot(),
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try external.BatchSource.init(external_binding, &batches);

    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.embedding.vector",
        .binding = .{
            .sidecar_kind = .vector,
            .source_kind = .external_iceberg,
            .row_ref_kind = .external,
            .source_id = "events",
            .snapshot_id = "iceberg-9",
            .schema_fingerprint = "schema-v2",
            .column_bindings = &[_][]const u8{"embedding"},
            .index_config_hash = "sha256:vector",
        },
        .artifact = .{
            .kind = artifact_ref.ArtifactKind.vector_segment,
            .name = "events.embedding.vector",
            .artifact_id = "vector-1",
            .byte_len = 128,
            .checksum = "len:128",
        },
    };
    const candidate_refs = [_]rowsource.RowRef{ row_refs[2], row_refs[1] };
    const deleted = [_]rowsource.RowRef{row_refs[2]};
    const candidates = [_]SidecarCandidateSet{.{
        .sidecar_name = "events.embedding.vector",
        .row_refs = &candidate_refs,
    }};
    const projection = [_][]const u8{"amount"};

    var result = try scanRowsWithSidecarsAlloc(alloc, batch_source.rowSource(), .{
        .scan = .{ .projected_columns = &projection, .deleted_row_refs = &deleted },
        .base_source = .{ .external_iceberg = .{
            .format = .iceberg,
            .source_uri = "s3://bucket/warehouse/events",
            .snapshot_id = "iceberg-9",
            .schema_fingerprint = "schema-v2",
        } },
        .sidecars = &[_]sidecar_manifest.DeclaredArtifact{declaration},
        .desired_sidecars = &[_]lake_sidecar_selection.DesiredSidecar{.{ .kind = .vector }},
        .sidecar_policy = .{ .require_requested = true },
        .candidates = &candidates,
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(SidecarScanSource.sidecar_hydration, result.source);
    try std.testing.expectEqual(@as(u32, 1), result.total);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expect(rowRefsEqual(row_refs[1], result.rows[0].row_ref));
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
}

test "lake rows sidecar scan intersects multiple selected candidate sets" {
    const alloc = std.testing.allocator;
    const external = @import("../../storage/rowsource/external.zig");
    const artifact_ref = @import("../manifest/artifact_ref.zig");

    const external_binding = external.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
    };
    const row_refs = [_]rowsource.RowRef{
        try external.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external.makeRowRef(external_binding, "file-a.parquet", 0, 1),
        try external.makeRowRef(external_binding, "file-b.parquet", 1, 0),
    };
    const amounts = [_]i64{ 10, 20, 30 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = external_binding.snapshot(),
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try external.BatchSource.init(external_binding, &batches);

    const vector_declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.embedding.vector",
        .binding = .{
            .sidecar_kind = .vector,
            .source_kind = .external_iceberg,
            .row_ref_kind = .external,
            .source_id = "events",
            .snapshot_id = "iceberg-9",
            .schema_fingerprint = "schema-v2",
            .column_bindings = &[_][]const u8{"embedding"},
            .index_config_hash = "sha256:vector",
        },
        .artifact = .{
            .kind = artifact_ref.ArtifactKind.vector_segment,
            .name = "events.embedding.vector",
            .artifact_id = "vector-1",
            .byte_len = 128,
            .checksum = "len:128",
        },
    };
    const text_declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.body.text",
        .binding = .{
            .sidecar_kind = .text,
            .source_kind = .external_iceberg,
            .row_ref_kind = .external,
            .source_id = "events",
            .snapshot_id = "iceberg-9",
            .schema_fingerprint = "schema-v2",
            .column_bindings = &[_][]const u8{"body"},
            .index_config_hash = "sha256:text",
        },
        .artifact = .{
            .kind = artifact_ref.ArtifactKind.text_segment,
            .name = "events.body.text",
            .artifact_id = "text-1",
            .byte_len = 64,
            .checksum = "len:64",
        },
    };
    const sidecars = [_]sidecar_manifest.DeclaredArtifact{ vector_declaration, text_declaration };
    const vector_refs = [_]rowsource.RowRef{ row_refs[2], row_refs[1] };
    const text_refs = [_]rowsource.RowRef{ row_refs[1], row_refs[0] };
    const candidates = [_]SidecarCandidateSet{
        .{ .sidecar_name = "events.embedding.vector", .row_refs = &vector_refs },
        .{ .sidecar_name = "events.body.text", .row_refs = &text_refs },
    };
    const desired = [_]lake_sidecar_selection.DesiredSidecar{
        .{ .name = "events.embedding.vector" },
        .{ .name = "events.body.text" },
    };
    const projection = [_][]const u8{"amount"};

    var result = try scanRowsWithSidecarsAlloc(alloc, batch_source.rowSource(), .{
        .scan = .{ .projected_columns = &projection },
        .base_source = .{ .external_iceberg = .{
            .format = .iceberg,
            .source_uri = "s3://bucket/warehouse/events",
            .snapshot_id = "iceberg-9",
            .schema_fingerprint = "schema-v2",
        } },
        .sidecars = &sidecars,
        .desired_sidecars = &desired,
        .sidecar_policy = .{ .require_requested = true },
        .candidates = &candidates,
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(SidecarScanSource.sidecar_hydration, result.source);
    try std.testing.expectEqual(@as(u32, 2), result.sidecar_selection.selected_count);
    try std.testing.expectEqual(@as(u32, 1), result.total);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expect(rowRefsEqual(row_refs[1], result.rows[0].row_ref));
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
}

test "lake rows automatic sidecar scan requests candidate sidecars" {
    const alloc = std.testing.allocator;
    const external = @import("../../storage/rowsource/external.zig");
    const artifact_ref = @import("../manifest/artifact_ref.zig");

    const external_binding = external.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
    };
    const row_refs = [_]rowsource.RowRef{
        try external.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external.makeRowRef(external_binding, "file-a.parquet", 0, 1),
        try external.makeRowRef(external_binding, "file-b.parquet", 1, 0),
    };
    const amounts = [_]i64{ 10, 20, 30 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = external_binding.snapshot(),
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try external.BatchSource.init(external_binding, &batches);

    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.embedding.vector",
        .binding = .{
            .sidecar_kind = .vector,
            .source_kind = .external_iceberg,
            .row_ref_kind = .external,
            .source_id = "events",
            .snapshot_id = "iceberg-9",
            .schema_fingerprint = "schema-v2",
            .column_bindings = &[_][]const u8{"embedding"},
            .index_config_hash = "sha256:vector",
        },
        .artifact = .{
            .kind = artifact_ref.ArtifactKind.vector_segment,
            .name = "events.embedding.vector",
            .artifact_id = "vector-1",
            .byte_len = 128,
            .checksum = "len:128",
        },
    };
    const candidate_refs = [_]rowsource.RowRef{ row_refs[2], row_refs[1] };
    const candidates = [_]SidecarCandidateSet{.{
        .sidecar_name = "events.embedding.vector",
        .row_refs = &candidate_refs,
    }};
    const projection = [_][]const u8{"amount"};

    var result = try scanRowsWithAutomaticSidecarsAlloc(alloc, batch_source.rowSource(), .{
        .scan = .{ .projected_columns = &projection },
        .base_source = .{ .external_iceberg = .{
            .format = .iceberg,
            .source_uri = "s3://bucket/warehouse/events",
            .snapshot_id = "iceberg-9",
            .schema_fingerprint = "schema-v2",
        } },
        .sidecars = &[_]sidecar_manifest.DeclaredArtifact{declaration},
        .candidates = &candidates,
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(SidecarScanSource.sidecar_hydration, result.source);
    try std.testing.expectEqual(@as(u32, 1), result.sidecar_selection.selected_count);
    try std.testing.expectEqual(@as(u32, 2), result.total);
    try std.testing.expect(rowRefsEqual(row_refs[1], result.rows[0].row_ref));
    try std.testing.expect(rowRefsEqual(row_refs[2], result.rows[1].row_ref));
}

test "lake rows automatic sidecar scan uses plain scan without candidates" {
    const alloc = std.testing.allocator;
    const external = @import("../../storage/rowsource/external.zig");
    const artifact_ref = @import("../manifest/artifact_ref.zig");

    const external_binding = external.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
    };
    const row_refs = [_]rowsource.RowRef{
        try external.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external.makeRowRef(external_binding, "file-a.parquet", 0, 1),
    };
    const amounts = [_]i64{ 10, 20 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = external_binding.snapshot(),
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try external.BatchSource.init(external_binding, &batches);

    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.embedding.vector",
        .binding = .{
            .sidecar_kind = .vector,
            .source_kind = .external_iceberg,
            .row_ref_kind = .external,
            .source_id = "events",
            .snapshot_id = "iceberg-9",
            .schema_fingerprint = "schema-v2",
            .column_bindings = &[_][]const u8{"embedding"},
            .index_config_hash = "sha256:vector",
        },
        .artifact = .{
            .kind = artifact_ref.ArtifactKind.vector_segment,
            .name = "events.embedding.vector",
            .artifact_id = "vector-1",
            .byte_len = 128,
            .checksum = "len:128",
        },
    };
    const projection = [_][]const u8{"amount"};

    var result = try scanRowsWithAutomaticSidecarsAlloc(alloc, batch_source.rowSource(), .{
        .scan = .{ .projected_columns = &projection },
        .base_source = .{ .external_iceberg = .{
            .format = .iceberg,
            .source_uri = "s3://bucket/warehouse/events",
            .snapshot_id = "iceberg-9",
            .schema_fingerprint = "schema-v2",
        } },
        .sidecars = &[_]sidecar_manifest.DeclaredArtifact{declaration},
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(SidecarScanSource.rowsource_scan, result.source);
    try std.testing.expectEqual(@as(u32, 0), result.sidecar_selection.selected_count);
    try std.testing.expectEqual(@as(u32, 2), result.total);
    try std.testing.expectEqual(@as(i64, 10), result.rows[0].find("amount").?.value.?.i64);
    try std.testing.expectEqual(@as(i64, 20), result.rows[1].find("amount").?.value.?.i64);
}

test "lake rows sidecar scan falls back when stale sidecars are ignored" {
    const alloc = std.testing.allocator;
    const external = @import("../../storage/rowsource/external.zig");
    const artifact_ref = @import("../manifest/artifact_ref.zig");

    const external_binding = external.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
    };
    const row_refs = [_]rowsource.RowRef{
        try external.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external.makeRowRef(external_binding, "file-a.parquet", 0, 1),
    };
    const tenants = [_][]const u8{ "t1", "t2" };
    const amounts = [_]i64{ 10, 20 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = external_binding.snapshot(),
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var batch_source = try external.BatchSource.init(external_binding, &batches);

    const stale_declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.embedding.vector",
        .binding = .{
            .sidecar_kind = .vector,
            .source_kind = .external_iceberg,
            .row_ref_kind = .external,
            .source_id = "events",
            .snapshot_id = "iceberg-8",
            .schema_fingerprint = "schema-v2",
            .column_bindings = &[_][]const u8{"embedding"},
            .index_config_hash = "sha256:vector",
        },
        .artifact = .{
            .kind = artifact_ref.ArtifactKind.vector_segment,
            .name = "events.embedding.vector",
            .artifact_id = "vector-old",
            .byte_len = 128,
            .checksum = "len:128",
        },
    };
    const stale_candidate_refs = [_]rowsource.RowRef{.{ .external = .{
        .source_id = "events",
        .snapshot_id = "iceberg-8",
        .file_id = "file-a.parquet",
        .row_group_ordinal = 0,
        .row_ordinal = 1,
    } }};
    const stale_candidates = [_]SidecarCandidateSet{.{
        .sidecar_name = "events.embedding.vector",
        .row_refs = &stale_candidate_refs,
    }};
    const projection = [_][]const u8{"amount"};

    var result = try scanRowsWithSidecarsAlloc(alloc, batch_source.rowSource(), .{
        .scan = .{
            .projected_columns = &projection,
            .predicate = .{
                .column = "tenant",
                .op = .eq_bytes,
                .bytes_value = "t2",
            },
        },
        .base_source = .{ .external_iceberg = .{
            .format = .iceberg,
            .source_uri = "s3://bucket/warehouse/events",
            .snapshot_id = "iceberg-9",
            .schema_fingerprint = "schema-v2",
        } },
        .sidecars = &[_]sidecar_manifest.DeclaredArtifact{stale_declaration},
        .desired_sidecars = &[_]lake_sidecar_selection.DesiredSidecar{.{ .kind = .vector }},
        .sidecar_policy = .{ .stale = .ignore },
        .candidates = &stale_candidates,
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(SidecarScanSource.rowsource_scan, result.source);
    try std.testing.expectEqual(@as(u32, 0), result.sidecar_selection.selected_count);
    try std.testing.expectEqual(@as(u32, 1), result.sidecar_selection.stale_ignored_count);
    try std.testing.expectEqual(@as(u32, 1), result.total);
    try std.testing.expect(rowRefsEqual(row_refs[1], result.rows[0].row_ref));
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
}

test "lake rows hydrates projected cells from serverless row fragments" {
    const alloc = std.testing.allocator;
    const row_fragment = @import("../row_fragment/mod.zig");

    var fragment = row_fragment.Fragment{
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .row_refs = try alloc.alloc(row_fragment.RowRef, 2),
        .columns = try alloc.alloc(row_fragment.Column, 1),
    };
    defer fragment.deinit(alloc);
    fragment.row_refs[0] = .{ .key = try alloc.dupe(u8, "row:a"), .ordinal = 0 };
    fragment.row_refs[1] = .{ .key = try alloc.dupe(u8, "row:b"), .ordinal = 1 };
    fragment.columns[0] = .{
        .name = try alloc.dupe(u8, "amount"),
        .kind = .i64,
        .values = try alloc.alloc(row_fragment.CellValue, 2),
    };
    fragment.columns[0].values[0] = .{ .i64 = 10 };
    fragment.columns[0].values[1] = .{ .i64 = 20 };

    var fragment_source = row_fragment.FragmentSource.init(
        .{ .table_id = "orders", .snapshot_id = "manifest-7" },
        "frag-1",
        &fragment,
    );
    defer fragment_source.deinit(alloc);

    const wanted = [_]rowsource.RowRef{.{ .serverless = .{
        .fragment_id = "frag-1",
        .row_ordinal = 1,
    } }};
    const projection = [_][]const u8{"amount"};
    var result = try hydrateRowsAlloc(alloc, fragment_source.rowSource(), &wanted, &projection);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expect(rowRefsEqual(wanted[0], result.rows[0].row_ref));
    try std.testing.expectEqual(@as(i64, 20), result.rows[0].find("amount").?.value.?.i64);
}
