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
    defaults: expressions.Set.DefaultsPolicy = .preserve_absence,
    dropped_columns: enum(u8) { reject, allow } = .reject,
};

/// Hard per-program/row ceilings supplement the scalar evaluator's shared
/// 4 MiB work budget. Wide unaffected cells are borrowed, not decoded to JSON.
pub const max_schema_bytes = 4 * 1024 * 1024;
pub const max_row_bytes = 16 * 1024 * 1024;
pub const max_columns = 4096;

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
    source_schema_digest: Digest,
    target_schema_digest: Digest,
    identity: Digest,

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
        const mapping = try alloc.alloc(?u32, to.len);
        errdefer alloc.free(mapping);
        for (to, mapping) |column, *ordinal| {
            const old = source.physicalLayout().ordinalForName(from, column.name);
            ordinal.* = if (old) |index| @intCast(index) else null;
            if (old) |index| {
                const previous = from[index];
                if (previous.column_type != column.column_type or previous.is_json != column.is_json or previous.json_kind != column.json_kind)
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
        hash.update("antfly.relational-row-transform.v1\x00");
        hash.update(&source_digest);
        hash.update(&target_digest);
        hash.update(&.{ @intFromEnum(policies.defaults), @intFromEnum(policies.dropped_columns) });
        var identity: Digest = undefined;
        hash.final(&identity);
        return .{ .alloc = alloc, .source = source, .target = target, .source_ordinals = mapping, .policies = policies, .source_schema_digest = source_digest, .target_schema_digest = target_digest, .identity = identity };
    }

    pub fn deinit(self: *Program) void {
        self.source.release();
        self.target.release();
        self.alloc.free(self.source_ordinals);
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
        return self.transformInternal(alloc, source_bytes) catch |err| return memoryError(err);
    }

    fn transformInternal(self: *const Program, alloc: Allocator, source_bytes: []const u8) !Result {
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
                values[ordinal] = try scalarValue(column.column_type, cell);
            }
        }
        if (expression_set) |set| {
            try set.applyValuesWithPolicy(arena, values, present, self.policies.defaults);
            for (set.bindings) |binding| {
                if (!binding.generated and cells[binding.ordinal] != null) continue;
                if (!present[binding.ordinal]) continue;
                cells[binding.ordinal] = try scalarCell(to.relational_columns[binding.ordinal], @intCast(binding.ordinal), values[binding.ordinal]);
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
        try validator.validateValue(alloc, &logical.root);
    } else {
        try store.validateCanonicalValueForSchemaAndLayout(alloc, bytes, view.tableSchema().*, view.physicalLayout());
        const row = try codec.ordinalRowViewTrusted(bytes, view.tableSchema().*, view.physicalLayout());
        try validator.validateRelationalRestoreFields(alloc, row);
    }
}

fn scalarValue(kind: schema.RelationalColumnType, cell: codec.Cell) !expressions.Value {
    if (cell.is_null) return .null;
    return switch (kind) {
        .string => .{ .string = cell.value.bytes_val },
        .blob => .{ .blob = cell.value.bytes_val },
        .integer => .{ .integer = cell.value.i64_val },
        .number => .{ .number = cell.value.f64_val },
        .boolean => .{ .boolean = cell.value.bool_val },
        .datetime => .{ .datetime = cell.value.u64_val },
        else => error.InvalidRelationalExpressionInput,
    };
}

fn scalarCell(column: schema.RelationalColumn, ordinal: u32, value: expressions.Value) !codec.Cell {
    const value_type: @import("../../section/typed_doc_values.zig").ValueType = switch (column.column_type) {
        .string, .blob => .bytes_val,
        .integer => .i64_val,
        .number => .f64_val,
        .boolean => .bool_val,
        .datetime => .u64_val,
        else => return error.InvalidRelationalExpressionInput,
    };
    return .{ .ordinal = ordinal, .path = column.path, .value_type = value_type, .is_null = value == .null, .value = switch (value) {
        .null => .{ .bytes_val = "" },
        .string, .blob => |bytes| .{ .bytes_val = bytes },
        .integer => |integer| .{ .i64_val = integer },
        .number => |number| .{ .f64_val = number },
        .boolean => |boolean| .{ .bool_val = boolean },
        .datetime => |datetime| .{ .u64_val = datetime },
    } };
}
