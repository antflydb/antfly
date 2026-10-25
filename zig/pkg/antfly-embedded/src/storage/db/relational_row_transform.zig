// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Immutable, deterministic row contract for explicit schema rewrites. This is
//! NOT ordinary restore: restore preserves historical values. A coordinator
//! must durably bind Program.identity before using results in a hidden target,
//! retain the source mutation tail, and fence/cut over the whole target cohort.
//! This module grants no publication authority and does not relax ALTER guards.
const std = @import("std");
const schema = @import("../schema.zig");
const schema_api = @import("../../schema/mod.zig");
const expressions = @import("../../schema/relational_expression.zig");
const registry = @import("schema_registry.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const store = @import("relational_store.zig");
const content = @import("document_content_hash.zig");
const Allocator = std.mem.Allocator;
pub const Digest = [32]u8;

pub const Policies = struct {
    default_columns: []const []const u8 = &.{},
    /// Captured by admission; shared by snapshot, tail and cold recovery.
    default_clock_micros: ?i64 = null,
    dropped_columns: enum(u8) { reject, allow } = .reject,
    expression_schema: []const u8 = "",
    column_transforms: []const @import("relational_rewrite_contract.zig").ColumnTransform = &.{},
};

/// Hard per-program/row ceilings supplement the scalar evaluator's shared
/// 4 MiB work budget. Wide unaffected cells are borrowed, not decoded to JSON.
pub const max_schema_bytes = 4 * 1024 * 1024;
pub const max_row_bytes = 16 * 1024 * 1024;
pub const max_columns = 4096;

/// All historical physical layouts share one immutable expression program.
/// A wide predecessor schema must not be cloned once per retained epoch.
const TransformBinding = struct { ordinal: u32, generated: bool, plan: expressions.Plan };

const TransformSet = struct {
    refs: std.atomic.Value(usize) = .init(1),
    alloc: Allocator,
    arena: std.heap.ArenaAllocator,
    source: registry.SchemaView,
    bindings: []TransformBinding,
    rules: []const @import("relational_rewrite_contract.zig").ColumnTransform,
    source_json: []const u8,
    digest: Digest,

    fn retain(self: *@This()) *@This() {
        _ = self.refs.fetchAdd(1, .monotonic);
        return self;
    }
    fn release(self: *@This()) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        for (self.bindings) |*binding| binding.plan.deinit();
        self.alloc.free(self.bindings);
        self.source.release();
        self.arena.deinit();
        self.alloc.destroy(self);
    }
    fn init(alloc: Allocator, target: registry.SchemaView, policies: Policies) !*@This() {
        if (policies.expression_schema.len > max_schema_bytes) return error.RelationalRewriteBudgetExceeded;
        var region = std.heap.ArenaAllocator.init(alloc);
        errdefer region.deinit();
        const a = region.allocator();
        const bytes = try a.dupe(u8, policies.expression_schema);
        var source = try compileView(alloc, bytes);
        errdefer source.release();
        const bindings = try alloc.alloc(TransformBinding, policies.column_transforms.len);
        errdefer alloc.free(bindings);
        var initialized: usize = 0;
        errdefer for (bindings[0..initialized]) |*binding| binding.plan.deinit();
        const rules = try a.alloc(@import("relational_rewrite_contract.zig").ColumnTransform, bindings.len);
        const columns = target.tableSchema().relational_columns;
        var size = bytes.len;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-column-rewrite-expressions-v1");
        hash.update(&try schemaDigest(alloc, bytes, source));
        for (policies.column_transforms, bindings, rules) |rule, *binding, *owned| {
            size +|= rule.column.len +| rule.expression_json.len;
            if (size > max_schema_bytes) return error.RelationalRewriteBudgetExceeded;
            const ordinal = target.physicalLayout().ordinalForName(columns, rule.column) orelse return error.InvalidRestoreStagingCommand;
            for (bindings[0..initialized]) |prior| if (prior.ordinal == ordinal) return error.InvalidRestoreStagingCommand;
            if (target.validator().?.execution.expressions) |set| if (set.generated_columns[ordinal]) return error.InvalidRestoreStagingCommand;
            owned.* = .{ .column = try a.dupe(u8, rule.column), .expression_json = try a.dupe(u8, rule.expression_json) };
            const expression = try std.json.parseFromSliceLeaky(std.json.Value, a, owned.expression_json, .{});
            binding.* = .{ .ordinal = @intCast(ordinal), .generated = false, .plan = try expressions.Plan.init(alloc, source.tableSchema().*, expression, columns[ordinal].column_type) };
            initialized += 1;
            // Numeric widths and array elements affect physical semantics.
            // Text/boolean/JSON operators may omit a redundant builtin tag;
            // their exact result kind and target validators remain required.
            switch (columns[ordinal].column_type) {
                .integer, .number, .numeric, .sql_array => if (columns[ordinal].sql_element_type) |kind| {
                    if (binding.plan.result_sql_type != kind) return error.RelationalRewriteTypeChange;
                },
                else => {},
            }
            var length: [8]u8 = undefined;
            std.mem.writeInt(u64, &length, owned.column.len, .little);
            hash.update(&length);
            hash.update(owned.column);
            hash.update(&binding.plan.fingerprint);
        }
        var digest: Digest = undefined;
        hash.final(&digest);
        const result = try alloc.create(@This());
        result.* = .{ .alloc = alloc, .arena = region, .source = source, .bindings = bindings, .rules = rules, .source_json = bytes, .digest = digest };
        return result;
    }
};

pub const Result = struct {
    packed_row: []u8,
    semantic_hash: Digest,
    source_semantic_hash: Digest,
    source_physical_digest: Digest,
    program_identity: Digest,
    /// Only scalar dependencies/results enter expression evaluation. This is
    /// useful for asserting projection behavior without wall-clock benchmarks.
    expression_input_cells: usize,

    pub fn deinit(self: *Result, alloc: Allocator) void {
        alloc.free(self.packed_row);
        self.* = undefined;
    }
};

pub const Program = struct {
    alloc: Allocator,
    source: registry.SchemaView,
    target: registry.SchemaView,
    /// target ordinal -> independently bound source ordinal. Numeric schema
    /// versions need not differ: the two immutable registries never alias.
    source_ordinals: []?u32,
    policies: Policies,
    default_mask: ?[]bool,
    default_transaction_start: ?@import("../../sql/statement_clock.zig").Instant,
    requires_materialized_defaults: bool,
    source_schema_digest: Digest,
    target_schema_digest: Digest,
    identity: Digest,
    transforms: []const TransformBinding,
    transform_set: ?*TransformSet,
    expression_ordinals: []?u32,

    pub fn init(alloc: Allocator, source_json: []const u8, target_json: []const u8, policies: Policies) !Program {
        return initInternal(alloc, source_json, target_json, policies, null) catch |err| return memoryError(err);
    }

    /// Historical source layouts share one immutable compiled target. The
    /// retained target reference survives independently of the template.
    pub fn initWithTarget(alloc: Allocator, source_json: []const u8, template: *const Program) !Program {
        return initInternal(alloc, source_json, "", template.policies, template) catch |err| return memoryError(err);
    }

    fn initInternal(alloc: Allocator, source_json: []const u8, target_json: []const u8, policies: Policies, template: ?*const Program) !Program {
        if (source_json.len > max_schema_bytes or target_json.len > max_schema_bytes) return error.RelationalRewriteBudgetExceeded;
        var source = try compileView(alloc, source_json);
        errdefer source.release();
        var target = if (template) |existing| existing.target.clone() else try compileView(alloc, target_json);
        errdefer target.release();
        const from = source.tableSchema().relational_columns;
        const to = target.tableSchema().relational_columns;
        if (from.len > max_columns or to.len > max_columns) return error.RelationalRewriteBudgetExceeded;
        if (policies.column_transforms.len > 256 or (policies.column_transforms.len == 0) != (policies.expression_schema.len == 0)) return error.InvalidRestoreStagingCommand;
        const transform_set: ?*TransformSet = if (policies.column_transforms.len == 0) null else if (template) |existing| existing.transform_set.?.retain() else try TransformSet.init(alloc, target, policies);
        errdefer if (transform_set) |set| set.release();
        const transforms: []const TransformBinding = if (transform_set) |set| set.bindings else &.{};
        const logical_columns: []const schema.RelationalColumn = if (transform_set) |set| set.source.tableSchema().relational_columns else &.{};
        const expression_ordinals = try alloc.alloc(?u32, logical_columns.len);
        errdefer alloc.free(expression_ordinals);
        for (logical_columns, expression_ordinals) |column, *ordinal| {
            ordinal.* = if (source.physicalLayout().ordinalForName(from, column.name)) |index| @intCast(index) else null;
        }
        // Resolve once, retaining names from our owned target view. Snapshot
        // and tail rows use the same ordinal mask without string lookups.
        if (policies.default_columns.len > to.len) return error.InvalidRestoreStagingCommand;
        const mask = if (policies.default_columns.len != 0) try alloc.alloc(bool, to.len) else null;
        errdefer if (mask) |value| alloc.free(value);
        const names = try alloc.alloc([]const u8, policies.default_columns.len);
        errdefer alloc.free(names);
        if (mask) |value| {
            @memset(value, false);
            for (policies.default_columns, names) |name, *owned_name| {
                const ordinal = target.physicalLayout().ordinalForName(to, name) orelse return error.InvalidRestoreStagingCommand;
                if (value[ordinal]) return error.InvalidRestoreStagingCommand;
                const set = target.validator().?.execution.expressions orelse return error.InvalidRestoreStagingCommand;
                const has_default = for (set.bindings) |binding| {
                    if (binding.ordinal == ordinal and !binding.generated) break true;
                } else false;
                if (!has_default) return error.InvalidRestoreStagingCommand;
                value[ordinal] = true;
                owned_name.* = to[ordinal].name;
            }
        }
        var clock_required = false;
        if (mask) |enabled| if (target.validator().?.execution.expressions) |set| {
            for (set.bindings) |binding| {
                if (binding.generated or !enabled[binding.ordinal]) continue;
                const plan = switch (binding.source) {
                    .expression => |*value| value,
                    // Rewrites must receive already materialized sequence
                    // defaults from their durable preparation owner.
                    .sequence => return error.UnsupportedSqlShape,
                };
                for (plan.nodes) |node| if (node.op == .transaction_timestamp) {
                    clock_required = true;
                    break;
                };
            }
        };
        if (clock_required != (policies.default_clock_micros != null)) return error.InvalidRestoreStagingCommand;
        const default_transaction_start = if (policies.default_clock_micros) |micros| instant: {
            const value = try @import("../../sql/statement_clock.zig").Instant.init(micros);
            if (value.infinity() != 0) return error.InvalidRestoreStagingCommand;
            break :instant value;
        } else null;
        const mapping = try alloc.alloc(?u32, to.len);
        errdefer alloc.free(mapping);
        for (to, mapping, 0..) |column, *ordinal, target_ordinal| {
            const replaced = for (transforms) |binding| {
                if (binding.ordinal == target_ordinal) break true;
            } else false;
            if (replaced) {
                ordinal.* = null;
                continue;
            }
            const old = source.physicalLayout().ordinalForName(from, column.name);
            ordinal.* = if (old) |index| @intCast(index) else null;
            if (old) |index| {
                const previous = from[index];
                if (previous.column_type != column.column_type or previous.is_json != column.is_json or previous.json_kind != column.json_kind or previous.sql_element_type != column.sql_element_type)
                    return error.RelationalRewriteTypeChange;
            }
        }
        if (policies.dropped_columns == .reject) for (from) |column| {
            if (target.physicalLayout().ordinalForName(to, column.name) == null)
                return error.RelationalRewriteColumnDrop;
        };
        const source_digest = try schemaDigest(alloc, source_json, source);
        const target_digest = if (template) |existing| existing.target_schema_digest else try schemaDigest(alloc, target_json, target);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly.relational-row-transform.v2\x00");
        hash.update(&source_digest);
        hash.update(&target_digest);
        hash.update(&.{ @backingInt(policies.dropped_columns), @intFromBool(mask != null) });
        if (mask) |value| {
            hash.update("scoped-defaults-v1");
            for (value) |enabled| hash.update(&.{@intFromBool(enabled)});
        }
        if (transforms.len != 0) {
            hash.update("explicit-column-transforms-v1");
            hash.update(&transform_set.?.digest);
        }
        if (policies.default_clock_micros) |micros| {
            hash.update("scoped-default-clock-v1");
            var encoded: [8]u8 = undefined;
            std.mem.writeInt(i64, &encoded, micros, .little);
            hash.update(&encoded);
        }
        var materialized_defaults = false;
        if (mask) |enabled| if (target.validator().?.execution.expressions) |set| {
            for (set.bindings) |binding| {
                if (binding.generated or !enabled[binding.ordinal]) continue;
                const plan = switch (binding.source) {
                    .expression => |*value| value,
                    .sequence => return error.UnsupportedSqlShape,
                };
                for (plan.nodes) |node| if (node.op == .uuidv4 or node.op == .uuidv7) {
                    materialized_defaults = true;
                    break;
                };
            }
        };
        var identity: Digest = undefined;
        hash.final(&identity);
        return .{ .requires_materialized_defaults = materialized_defaults, .alloc = alloc, .source = source, .target = target, .source_ordinals = mapping, .policies = .{ .default_columns = names, .default_clock_micros = policies.default_clock_micros, .dropped_columns = policies.dropped_columns, .expression_schema = if (transform_set) |set| set.source_json else "", .column_transforms = if (transform_set) |set| set.rules else &.{} }, .default_mask = mask, .default_transaction_start = default_transaction_start, .source_schema_digest = source_digest, .target_schema_digest = target_digest, .identity = identity, .transforms = transforms, .transform_set = transform_set, .expression_ordinals = expression_ordinals };
    }

    pub fn deinit(self: *Program) void {
        if (self.transform_set) |set| set.release();
        self.source.release();
        self.target.release();
        self.alloc.free(self.source_ordinals);
        self.alloc.free(self.expression_ordinals);
        self.alloc.free(self.policies.default_columns);
        if (self.default_mask) |mask| self.alloc.free(mask);
        self.* = undefined;
    }

    /// The caller has independently verified the artifact's public/native
    /// layout pair. Numeric versions and compatible packed cell layouts alone
    /// do not authenticate the semantic source definition.
    pub fn requireSourceDefinition(self: *const Program, alloc: Allocator, json: []const u8) !void {
        var view = try compileView(alloc, json);
        defer view.release();
        const actual = try schemaDigest(alloc, json, view);
        if (!std.mem.eql(u8, &actual, &self.source_schema_digest)) return error.RestoreStagingScopeChanged;
    }

    /// Produces owned bytes suitable for target PreparedRelationalWrite's
    /// durable-row path. No side effects occur here; row/index/counter/outbox
    /// effects and the receiver cursor must commit together at the caller.
    /// The source timestamp survives snapshot and tail replay unchanged.
    pub fn transform(self: *const Program, alloc: Allocator, source_bytes: []const u8) !Result {
        return self.transformInternal(alloc, source_bytes, null, null) catch |err| return memoryError(err);
    }

    /// Only the staging owner supplies invocation I/O and a previously committed
    /// target row. Newly added defaults survive source updates; generated values
    /// still recompute from the new source afterimage. The caller atomically
    /// commits these owned bytes with its durable replay cursor.
    pub fn transformPrepared(self: *const Program, alloc: Allocator, source_bytes: []const u8, runtime: ?expressions.PreparationRuntime, previous_target: ?[]const u8) !Result {
        return self.transformInternal(alloc, source_bytes, runtime, previous_target) catch |err| return memoryError(err);
    }

    fn transformInternal(self: *const Program, alloc: Allocator, source_bytes: []const u8, runtime: ?expressions.PreparationRuntime, previous_target: ?[]const u8) !Result {
        if (source_bytes.len > max_row_bytes) return error.RelationalRewriteBudgetExceeded;
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const arena = scratch.allocator();
        const from = self.source.tableSchema().*;
        const to = self.target.tableSchema().*;

        // Validate before mapping or applying defaults. A forged historical
        // generated value must fail, never be silently repaired by the target.
        try validateStored(alloc, source_bytes, self.source);
        const row = try codec.ordinalRowViewTrusted(source_bytes, from, self.source.physicalLayout());
        const cells = try arena.alloc(?codec.Cell, to.relational_columns.len);
        const values = try arena.alloc(expressions.Value, cells.len);
        const present = try arena.alloc(bool, cells.len);
        @memset(cells, null);
        @memset(values, .null);
        @memset(present, false);
        const expression_set = self.target.validator().?.execution.expressions;
        var expression_bytes: usize = expressions.max_allocated_bytes;
        var execution = expressions.Execution.init(arena, &expression_bytes);
        execution.preparation_runtime = runtime;
        execution.bound_transaction_start = self.default_transaction_start;
        var input: expressions.PreparedInput = undefined;
        try input.initBoundRow(&execution, row);
        defer input.deinit();
        var expression_input_cells: usize = 0;
        for (to.relational_columns, self.source_ordinals, 0..) |column, old, ordinal| {
            const old_ordinal = old orelse continue;
            var cell = (try row.findCell(old_ordinal)) orelse continue;
            cell.ordinal = @intCast(ordinal);
            cell.path = column.path;
            cells[ordinal] = cell;
            present[ordinal] = true;
            if (expression_set) |set| {
                if (!set.read_columns[ordinal] or set.generated_columns[ordinal]) continue;
                expression_input_cells += 1;
                values[ordinal] = try input.valueAt(&execution, old_ordinal);
            }
        }
        if (self.transforms.len != 0) {
            var operands: expressions.PreparedInput = undefined;
            // Each immutable physical layout resolves names once, before any
            // source pin. Row execution uses direct mapped ordinals; absent
            // old fields remain SQL NULL without a per-row name lookup.
            try operands.initMappedRow(&execution, row, self.expression_ordinals);
            defer operands.deinit();
            for (self.transforms) |binding| {
                const result = try binding.plan.evaluatePreparedWithExecution(&execution, &operands);
                cells[binding.ordinal] = try scalarCell(&execution, to, to.relational_columns[binding.ordinal], binding.ordinal, result);
                values[binding.ordinal] = result;
                present[binding.ordinal] = true;
                expression_input_cells += binding.plan.dependencies.len;
            }
        }
        if (self.requires_materialized_defaults) if (previous_target) |bytes| {
            try validateStored(alloc, bytes, self.target);
            const previous = try codec.ordinalRowViewTrusted(bytes, to, self.target.physicalLayout());
            var previous_input: expressions.PreparedInput = undefined;
            try previous_input.initBoundRow(&execution, previous);
            defer previous_input.deinit();
            for (expression_set.?.bindings) |binding| {
                const ordinal = binding.ordinal;
                if (binding.generated or !self.default_mask.?[ordinal] or present[ordinal]) continue;
                const cell = (try previous.findCell(ordinal)) orelse continue;
                cells[ordinal] = cell;
                values[ordinal] = try previous_input.valueAt(&execution, ordinal);
                present[ordinal] = true;
            }
        };
        if (expression_set) |set| {
            try set.applyValuesWithExecution(&execution, values, present, if (self.default_mask != null) .apply_to_absent else .preserve_absence, self.default_mask);
            for (set.bindings) |binding| {
                if (!binding.generated and cells[binding.ordinal] != null) continue;
                if (!present[binding.ordinal]) continue;
                cells[binding.ordinal] = try scalarCell(&execution, to, to.relational_columns[binding.ordinal], @intCast(binding.ordinal), values[binding.ordinal]);
            }
        }

        const packed_cells = try arena.alloc(codec.Cell, cells.len);
        var count: usize = 0;
        // Conservative physical upper bound, checked BEFORE allocating output.
        // Dense headers/offsets fit in 64 bytes/column plus a fixed allowance.
        var encoded_bound: usize = 1024 + cells.len * 64;
        for (cells) |optional| {
            const cell = optional orelse continue;
            if (!cell.is_null and cell.value == .bytes_val) {
                encoded_bound = std.math.add(usize, encoded_bound, cell.value.bytes_val.len) catch return error.RelationalRewriteBudgetExceeded;
            }
            packed_cells[count] = cell;
            count += 1;
        }
        // Include grouped-checksum storage with slack (the codec currently
        // uses four bytes per 4 KiB). Do not admit a payload-sized row whose
        // physical checksum directory would push it over the output limit.
        encoded_bound = std.math.add(usize, encoded_bound, encoded_bound / 256 + 64) catch return error.RelationalRewriteBudgetExceeded;
        if (encoded_bound > max_row_bytes) return error.RelationalRewriteBudgetExceeded;
        const semantic_hash = try content.hashRelationalCellsWithOrdinals(arena, cells, to, self.target.physicalLayout().hash_ordinals);
        const encoded = try codec.serializePreparedOrdinalDeferredHash(alloc, to.version, to.relational_columns, packed_cells[0..count], self.target.physicalLayout());
        errdefer alloc.free(encoded);
        if (encoded.len > max_row_bytes) return error.RelationalRewriteBudgetExceeded;
        try codec.finalizeOrdinalMetadata(encoded, semantic_hash, row.writeTimestampNs());
        // Shared strict target validation also checks narrowed required/null
        // constraints and CHECKs. Full DOM is only needed for full-root schemas.
        try validateStored(alloc, encoded, self.target);
        var physical_digest: Digest = undefined;
        std.crypto.hash.Blake3.hash(source_bytes, &physical_digest, .{});
        return .{ .packed_row = encoded, .semantic_hash = semantic_hash, .source_semantic_hash = row.semanticHash(), .source_physical_digest = physical_digest, .program_identity = self.identity, .expression_input_cells = expression_input_cells };
    }
};

fn memoryError(err: anyerror) anyerror {
    // These operations perform no I/O. std.Io.Writer.Allocating maps an
    // allocation failure to WriteFailed; preserve its retryable OOM meaning.
    return if (err == error.WriteFailed) error.OutOfMemory else err;
}

fn compileView(alloc: Allocator, json: []const u8) !registry.SchemaView {
    var validator = try schema_api.CompiledTableValidator.init(alloc, json);
    errdefer validator.deinit(alloc);
    const runtime = try schema_api.deriveRuntimeTableSchema(alloc, validator.schema);
    errdefer schema.freeSchema(alloc, runtime);
    if (runtime.storage_mode != .relational) return error.RelationalRewriteRequiresRelational;
    return .{ .epoch = try registry.Epoch.createOwnedValidated(alloc, runtime, validator) };
}

fn schemaDigest(alloc: Allocator, json: []const u8, view: registry.SchemaView) !Digest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{ .parse_numbers = false });
    defer parsed.deinit();
    const canonical = try content.canonicalJsonValueAlloc(alloc, parsed.value);
    defer alloc.free(canonical);
    const runtime = try schema.serializeSchema(alloc, view.tableSchema().*);
    defer alloc.free(runtime);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.relational-rewrite-schema.v1\x00");
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, canonical.len, .little);
    hash.update(&length);
    hash.update(canonical);
    hash.update(runtime);
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}

fn validateStored(alloc: Allocator, bytes: []const u8, view: registry.SchemaView) !void {
    const validator = view.validator().?;
    if (validator.restore.full_root) {
        var logical = try codec.validateCanonicalAndMaterializeOrdinalRootWithLayoutAlloc(alloc, bytes, view.tableSchema().*, view.physicalLayout());
        defer logical.deinit(alloc);
        try validator.validateTypedStoredRoot(alloc, &logical.root, try codec.ordinalRowViewTrusted(bytes, view.tableSchema().*, view.physicalLayout()));
    } else {
        try store.validateCanonicalValueForSchemaAndLayout(alloc, bytes, view.tableSchema().*, view.physicalLayout());
        const row = try codec.ordinalRowViewTrusted(bytes, view.tableSchema().*, view.physicalLayout());
        try validator.validateRelationalRestoreFields(alloc, row);
    }
}

fn scalarCell(execution: *expressions.Execution, table: schema.TableSchema, column: schema.RelationalColumn, ordinal: u32, value: expressions.Value) !codec.Cell {
    try execution.charge(1);
    if (value != .null and !(if (value == .range) @import("../../sql/range_value.zig").kindFromType(column.column_type) == value.range.kind else std.mem.eql(u8, @tagName(value), @tagName(column.column_type)))) return error.InvalidRelationalExpressionInput;
    if (value == .sql_array and (column.column_type != .sql_array or value.sql_array.element_type != column.sql_element_type)) return error.InvalidRelationalExpressionInput;
    const value_type: @import("../../section/typed_doc_values.zig").ValueType = switch (column.column_type) {
        .string, .blob, .numeric, .sql_array, .json, .numrange, .daterange, .tsrange, .tstzrange => .bytes_val,
        .integer => .i64_val,
        .number => .f64_val,
        .boolean => .bool_val,
        .datetime => .u64_val,
        else => return error.InvalidRelationalExpressionInput,
    };
    return .{
        .ordinal = ordinal,
        .path = column.path,
        .value_type = value_type,
        .is_json = column.is_json,
        .is_numeric = column.column_type == .numeric,
        .sql_range_kind = @import("../../sql/range_value.zig").kindFromType(column.column_type),
        .sql_array_element_type = if (column.column_type == .sql_array) column.sql_element_type else null,
        .is_null = value == .null,
        .value = switch (value) {
            .null => .{ .bytes_val = "" },
            .string, .numeric => |bytes| .{ .bytes_val = bytes },
            .blob => |bytes| blk: {
                const size = std.base64.standard.Encoder.calcSize(bytes.len);
                try execution.charge(size);
                if (size > execution.bytes.*) return execution.limit();
                const encoded = try execution.alloc.alloc(u8, size);
                execution.bytes.* -= size;
                _ = std.base64.standard.Encoder.encode(encoded, bytes);
                break :blk .{ .bytes_val = encoded };
            },
            .integer => |integer| .{ .i64_val = integer },
            .number => |number| .{ .f64_val = number },
            .boolean => |boolean| .{ .bool_val = boolean },
            .datetime => |datetime| .{ .u64_val = std.math.cast(u64, datetime) orelse return error.InvalidRelationalExpressionInput },
            .sql_array => |array| .{ .bytes_val = array.bytes },
            .range => |range| .{ .bytes_val = range.bytes },
            .json => |json| blk: {
                var memory: @import("../../sql/memory_budget.zig") = .{ .backing = execution.alloc, .limit = execution.bytes.*, .monotonic = true };
                defer execution.bytes.* -|= memory.footprint();
                var work: @import("../../sql/json_order.zig").Budget = .{ .shared = &execution.numeric };
                try @import("../../sql/json_order.zig").validateTextDomain(json.value, &work, 0);
                const encoded = (if (table.requires_jsonb_display)
                    content.canonicalJsonStorageValueAlloc(memory.allocator(), json.value)
                else
                    content.canonicalJsonValueAlloc(memory.allocator(), json.value)) catch |err| {
                    if (err == error.OutOfMemory and memory.isExhausted()) return execution.limit();
                    return err;
                };
                try execution.charge(encoded.len);
                break :blk .{ .bytes_val = encoded };
            },
        },
    };
}

test "relational index system typed rewrite output preserves JSON identity and shares admission" {
    const Run = struct {
        fn run(backing: Allocator) !void {
            var region = std.heap.ArenaAllocator.init(backing);
            defer region.deinit();
            const a = region.allocator();
            const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"n\":1.20}", .{ .parse_numbers = false });
            const json: @import("../../sql/row_value.zig").Json = .{ .value = parsed };
            const column: schema.RelationalColumn = .{ .name = "j", .path = "j", .column_type = .json, .is_json = true, .json_kind = .any };
            var bytes: usize = expressions.max_allocated_bytes;
            var execution = expressions.Execution.init(a, &bytes);
            const cell = try scalarCell(&execution, .{ .requires_jsonb_display = true }, column, 0, .{ .json = &json });
            try std.testing.expect(cell.is_json and !cell.is_null);
            try std.testing.expectEqualStrings("{\"n\":1.20}", cell.value.bytes_val);
            try std.testing.expect(bytes < expressions.max_allocated_bytes);
            const present_null: @import("../../sql/row_value.zig").Json = .{ .value = .null };
            const json_null = try scalarCell(&execution, .{}, column, 0, .{ .json = &present_null });
            try std.testing.expect(json_null.is_json and !json_null.is_null);
            try std.testing.expectEqualStrings("null", json_null.value.bytes_val);
            const sql_null = try scalarCell(&execution, .{}, column, 0, .null);
            try std.testing.expect(sql_null.is_json and sql_null.is_null);
            try std.testing.expectError(error.InvalidRelationalExpressionInput, scalarCell(&execution, .{}, column, 0, .{ .string = "null" }));
            bytes = 0;
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, scalarCell(&execution, .{}, column, 0, .{ .json = &json }));
            // Byte admission failure remains sticky even for a later NULL,
            // which needs no output allocation of its own.
            bytes = expressions.max_allocated_bytes;
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, scalarCell(&execution, .{}, column, 0, .null));
            var blob_bytes: usize = 3;
            var blob_execution = expressions.Execution.init(a, &blob_bytes);
            const blob_column: schema.RelationalColumn = .{ .name = "b", .path = "b", .column_type = .blob };
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, scalarCell(&blob_execution, .{}, blob_column, 0, .{ .blob = &.{ 0, 1 } }));
            try std.testing.expectEqual(@as(usize, 3), blob_bytes);
        }
    };
    try Run.run(std.testing.allocator);
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{});
}

test "relational index system array rewrite results retain canonical bytes and reject element reinterpretation" {
    var region = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer region.deinit();
    var bytes: usize = expressions.max_allocated_bytes;
    var execution = expressions.Execution.init(region.allocator(), &bytes);
    const column: schema.RelationalColumn = .{ .name = "a", .path = "a", .column_type = .sql_array, .sql_element_type = .int64 };
    const value: expressions.Value = .{ .sql_array = .{ .element_type = .int64, .bytes = &.{ 1, 0, 0, 0, 0, 0, 0, 0 } } };
    const cell = try scalarCell(&execution, .{}, column, 0, value);
    try std.testing.expectEqual(column.sql_element_type, cell.sql_array_element_type);
    try std.testing.expectEqual(value.sql_array.bytes.ptr, cell.value.bytes_val.ptr);
    var wrong = column;
    wrong.sql_element_type = .int32;
    try std.testing.expectError(error.InvalidRelationalExpressionInput, scalarCell(&execution, .{}, wrong, 0, value));
    const nullable = try scalarCell(&execution, .{}, column, 0, .null);
    try std.testing.expect(nullable.is_null);
}
