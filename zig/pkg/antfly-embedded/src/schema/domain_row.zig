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

//! Prepared domain-column operations for the shared row pipeline. The native
//! caller retains one plan per immutable epoch; no catalog/name lookup enters
//! row preparation. Schema admission and storage reader fencing remain separate.
const std = @import("std");
const values = @import("domain_value.zig");
const domains = @import("../system_catalog/domain_types.zig");
const types = @import("../system_catalog/named_types.zig");
const schema = @import("../storage/schema.zig");
const vm = @import("relational_expression.zig");
const A = std.mem.Allocator;

/// Internal compiled SQL/native-reader provenance. Public JSON and client
/// parameter descriptors cannot construct an already-typed value witness.
pub const Input = struct { ordinal: u32, source: ?domains.Reference = null, use_default: bool = false };

/// Trusted mutation metadata, separate from public JSON. INSERT validates every
/// destination cell; PATCH validates only assigned inputs and generated outputs.
/// Both may carry sparse witnesses from pinned SQL/native output descriptors.
pub const Admission = struct {
    schema_version: u32,
    inputs: ?[]const Input = null,
    complete_row: bool = true,

    pub fn inputAt(self: Admission, ordinal: usize) ?Input {
        const fields = self.inputs orelse return null;
        var lower: usize = 0;
        var upper = fields.len;
        while (lower < upper) {
            const middle = lower + (upper - lower) / 2;
            if (fields[middle].ordinal < ordinal) lower = middle + 1 else upper = middle;
        }
        return if (lower < fields.len and fields[lower].ordinal == ordinal) fields[lower] else null;
    }

    pub fn appliesDefault(self: Admission, ordinal: usize) bool {
        return self.complete_row or if (self.inputAt(ordinal)) |field| field.use_default else false;
    }
};

/// A destination fingerprint for a completed logical preparation. This is
/// not an authorization token: only authenticated internal handoffs and owned
/// SQL session records may attach it to a write. Canonical owner verification
/// still runs before the atomic row/index/intent boundary.
pub const PreparedContract = struct {
    version: u8 = 1,
    schema_version: u32,
    domain_digest: [32]u8,

    pub fn validate(self: PreparedContract) !void {
        if (self.version != 1 or std.mem.allEqual(u8, &self.domain_digest, 0)) return error.InvalidRelationalExpressionInput;
    }
    pub fn eql(self: PreparedContract, other: PreparedContract) bool {
        return self.version == other.version and self.schema_version == other.schema_version and std.mem.eql(u8, &self.domain_digest, &other.domain_digest);
    }
};

pub const Failure = struct { ordinal: u32, violation: values.Violation };
pub const Plan = struct {
    registry: values.Registry,
    owns_registry: bool = false,
    bindings: []Binding,
    column_count: usize,
    schema_version: u32,
    prepared_digest: [32]u8,
    alloc: A,
    const Binding = struct { ordinal: u32, name: []u8, view: *const values.View, domain_default: bool = true };

    pub fn init(a: A, table: schema.TableSchema) !Plan {
        var registry = try values.Registry.init(a, table.named_types);
        errdefer registry.deinit();
        var plan = try initBorrowed(a, table.version, table.storage_mode, table.relational_columns, &registry);
        plan.owns_registry = true;
        return plan;
    }

    /// The immutable schema owner must outlive this plan. Reuse its prepared
    /// DAG rather than cloning definitions and compiling every CHECK again.
    pub fn initBorrowed(a: A, version: u32, mode: schema.StorageMode, columns: []const schema.RelationalColumn, registry: *const values.Registry) !Plan {
        var bindings: std.ArrayList(Binding) = .empty;
        defer bindings.deinit(a);
        errdefer for (bindings.items) |binding| a.free(binding.name);
        for (columns, 0..) |column, ordinal| {
            const id = column.named_type_id orelse continue;
            const entry = registry.byId(id) orelse return error.InvalidSchema;
            if (entry.executable.? != .domain) continue;
            const view = &entry.executable.?.domain;
            // Names and ordinal positions belong to the table, while the exact
            // physical base belongs to the immutable domain definition.
            if (column.column_type != view.column.column_type or column.sql_element_type != view.column.sql_element_type or
                !@import("../common/sql_builtin_type.zig").NumericModifier.eql(column.numeric_modifier, view.column.numeric_modifier) or
                column.is_json != view.column.is_json or column.json_kind != view.column.json_kind) return error.InvalidSchema;
            const name = try a.dupe(u8, column.name);
            errdefer a.free(name);
            try bindings.append(a, .{ .ordinal = std.math.cast(u32, ordinal) orelse return error.InvalidSchema, .name = name, .view = view });
        }
        if (bindings.items.len != 0 and mode != .relational) return error.InvalidSchema;
        const digest = try preparedDigest(a, registry.records, columns);
        return .{ .registry = registry.*, .bindings = try bindings.toOwnedSlice(a), .column_count = columns.len, .schema_version = version, .prepared_digest = digest, .alloc = a };
    }

    /// Constant-time handoff checks: no row parsing, catalog lookup, or program
    /// compilation. Preparation froze the complete immutable type directory.
    pub fn preparedContract(self: *const Plan) PreparedContract {
        return .{ .schema_version = self.schema_version, .domain_digest = self.prepared_digest };
    }
    pub fn requirePrepared(self: *const Plan, contract: PreparedContract) !void {
        try contract.validate();
        if (self.bindings.len == 0 or !self.preparedContract().eql(contract)) return error.CatalogGenerationChanged;
    }

    pub fn deinit(self: *Plan) void {
        for (self.bindings) |binding| self.alloc.free(binding.name);
        self.alloc.free(self.bindings);
        if (self.owns_registry) self.registry.deinit();
        self.* = undefined;
    }

    /// Run before generated dependents and base modifier normalization. The
    /// existing row producer decides eligibility: an explicit column default
    /// takes precedence, and PATCH/restore must not fill historical omissions.
    /// Only absent eligible cells evaluate a domain default. An explicit SQL
    /// NULL remains an assignment and never invokes DEFAULT.
    pub fn prepareDefaults(self: *const Plan, execution: *vm.Execution, cells: []vm.Value, present: []bool, eligible: []const bool) !void {
        try self.requireWidths(cells.len, present.len, eligible.len);
        for (self.bindings) |binding| {
            try execution.charge(1);
            const ordinal = binding.ordinal;
            if (present[ordinal] or !eligible[ordinal]) continue;
            if (try binding.view.defaultValue(execution)) |value| {
                cells[ordinal] = value;
                present[ordinal] = true;
            }
        }
    }

    /// Call after base coercion/defaults/generated fields, before atomic row,
    /// index and outbox writes. `assigned` marks actual coercion boundaries;
    /// untouched historical cells must not start enforcing NOT VALID rules.
    /// A trusted SQL output descriptor may qualify an already-domain input.
    /// Raw JSON/COPY input supplies no witness and receives every domain rule.
    pub fn validateAssigned(self: *const Plan, execution: *vm.Execution, cells: []const vm.Value, present: []const bool, assigned: []const bool, provenance: ?[]const ?domains.Reference) !?Failure {
        try self.requireWidths(cells.len, present.len, assigned.len);
        if (provenance) |witnesses| if (witnesses.len != self.column_count) return error.InvalidRelationalExpressionInput;
        for (self.bindings) |binding| {
            const ordinal = binding.ordinal;
            try execution.charge(1);
            if (!assigned[ordinal]) continue;
            const witness = if (provenance) |witnesses| witnesses[ordinal] else null;
            if (!present[ordinal] and witness != null) return error.InvalidRelationalExpressionInput;
            if (try binding.view.validateCoercion(execution, if (present[ordinal]) cells[ordinal] else .null, witness)) |violation|
                return .{ .ordinal = ordinal, .violation = violation };
        }
        return null;
    }

    /// Called only while constructing the immutable plan. Existing explicit
    /// column defaults and generated expressions retain precedence over a
    /// domain default. No source expression is copied into table metadata.
    pub fn excludeDomainDefaults(self: *Plan, names: []const []const u8) void {
        for (self.bindings) |*binding| for (names) |name| if (std.mem.eql(u8, binding.name, name)) {
            binding.domain_default = false;
            break;
        };
    }

    pub fn markGenerated(self: *Plan, name: []const u8, derived: []bool) !void {
        if (derived.len != self.column_count) return error.InvalidRelationalExpressionInput;
        for (self.bindings) |*binding| if (std.mem.eql(u8, binding.name, name)) {
            derived[binding.ordinal] = true;
            binding.domain_default = false;
            return;
        };
    }

    /// Sparse JSON preparation shares the existing parsed document and null
    /// authority. It allocates only emitted defaults, never schema-width cells,
    /// masks or a second JSON document. Base normalization follows this step.
    pub fn prepareJsonDefaults(self: *const Plan, execution: *vm.Execution, document: *std.json.Value, nulls: *@import("../sql/row_value.zig").JsonNulls) !void {
        return self.prepareJsonDefaultsForAdmission(execution, document, nulls, .{ .schema_version = self.schema_version });
    }

    pub fn prepareJsonDefaultsForAdmission(self: *const Plan, execution: *vm.Execution, document: *std.json.Value, nulls: *@import("../sql/row_value.zig").JsonNulls, admission: Admission) !void {
        if (document.* != .object) return error.InvalidRelationalExpressionInput;
        try self.requireAdmission(admission);
        for (self.bindings) |binding| {
            try execution.charge(1);
            if (admission.inputAt(binding.ordinal)) |field| if (field.use_default and document.object.contains(binding.name)) return error.InvalidRelationalExpressionInput;
            if (!binding.domain_default or document.object.contains(binding.name) or !admission.appliesDefault(binding.ordinal)) continue;
            if (try binding.view.defaultValue(execution)) |value| {
                if (binding.name.len > execution.bytes.*) return execution.limit();
                execution.bytes.* -= binding.name.len;
                const name = try execution.alloc.dupe(u8, binding.name);
                const output = try vm.boundedValueToJson(execution, value);
                try document.object.put(execution.alloc, name, output);
                if (binding.view.column.column_type == .json) try nulls.assign(name, value);
            }
        }
    }

    /// Validate the sparse input description before applying any field. A
    /// descriptor is tied to one exact destination epoch, not merely an ordinal
    /// that could refer to a different column after a schema change.
    pub fn requireInputs(self: *const Plan, schema_version: u32, inputs: ?[]const Input) !void {
        if (schema_version != self.schema_version) return error.CatalogGenerationChanged;
        const fields = inputs orelse return;
        var previous: ?u32 = null;
        for (fields) |field| {
            if (field.ordinal >= self.column_count or (previous != null and field.ordinal <= previous.?)) return error.InvalidRelationalExpressionInput;
            previous = field.ordinal;
            if (field.use_default and field.source != null) return error.InvalidRelationalExpressionInput;
            if (field.source != null) {
                const binding = self.bindingAt(field.ordinal) orelse return error.InvalidRelationalExpressionInput;
                const identity = binding.view.identity orelse return error.InvalidSqlNamedType;
                const witness = field.source.?;
                if (identity.id != witness.id or identity.generation != witness.generation or !std.mem.eql(u8, &identity.origin_id, &witness.origin_id)) return error.CatalogGenerationChanged;
            }
        }
    }

    pub fn requireAdmission(self: *const Plan, admission: Admission) !void {
        if (!admission.complete_row and admission.inputs == null) return error.InvalidRelationalExpressionInput;
        try self.requireInputs(admission.schema_version, admission.inputs);
    }

    /// The schema compiler may pass a derived-column mask so base coercions
    /// happen before generated dependents, while generated/default outputs are
    /// checked after their own evaluation. Empty/absent inputs remain distinct:
    /// an empty list is an unrelated patch; null means fresh whole-row input.
    pub fn validateJsonAssigned(self: *const Plan, execution: *vm.Execution, document: std.json.Value, roots: *@import("../sql/row_value.zig").JsonRow, nulls: *@import("../sql/row_value.zig").JsonNulls, schema_version: u32, inputs: ?[]const Input, derived: ?[]const bool, selection: Selection) !?Failure {
        return self.validateJsonAdmission(execution, document, roots, nulls, .{ .schema_version = schema_version, .inputs = inputs, .complete_row = inputs == null }, derived, selection);
    }

    pub const Selection = enum { all, base, derived };
    pub fn validateJsonAdmission(self: *const Plan, execution: *vm.Execution, document: std.json.Value, roots: *@import("../sql/row_value.zig").JsonRow, nulls: *@import("../sql/row_value.zig").JsonNulls, admission: Admission, derived: ?[]const bool, selection: Selection) !?Failure {
        if (document != .object or (derived != null and derived.?.len != self.column_count)) return error.InvalidRelationalExpressionInput;
        try self.requireAdmission(admission);
        const inputs = admission.inputs;
        var cursor: usize = 0;
        for (self.bindings) |binding| {
            try execution.charge(1);
            const is_derived = if (derived) |mask| mask[binding.ordinal] else false;
            if ((selection == .base and is_derived) or (selection == .derived and !is_derived)) continue;
            var witness: ?domains.Reference = null;
            if (inputs) |fields| {
                while (cursor < fields.len and fields[cursor].ordinal < binding.ordinal) : (cursor += 1) {}
                const submitted = cursor < fields.len and fields[cursor].ordinal == binding.ordinal;
                if (!is_derived and !submitted and !admission.complete_row) continue;
                // Native derived expressions produce fresh base values. Their
                // values cannot inherit a submitted input descriptor witness.
                if (!is_derived and submitted) witness = fields[cursor].source;
            }
            const input = document.object.get(binding.name);
            if (input == null and witness != null) return error.InvalidRelationalExpressionInput;
            const value = if (input) |raw| try readValue(execution, binding, raw, roots, nulls) else vm.Value.null;
            if (try binding.view.validateCoercion(execution, value, witness)) |violation| return .{ .ordinal = binding.ordinal, .violation = violation };
        }
        return null;
    }

    fn bindingAt(self: *const Plan, ordinal: u32) ?*const Binding {
        var lower: usize = 0;
        var upper = self.bindings.len;
        while (lower < upper) {
            const middle = lower + (upper - lower) / 2;
            const current = &self.bindings[middle];
            if (current.ordinal == ordinal) return current;
            if (current.ordinal < ordinal) lower = middle + 1 else upper = middle;
        }
        return null;
    }

    fn readValue(execution: *vm.Execution, binding: Binding, input: std.json.Value, roots: *@import("../sql/row_value.zig").JsonRow, nulls: *@import("../sql/row_value.zig").JsonNulls) !vm.Value {
        if (input == .null and !(binding.view.column.column_type == .json and try nulls.contains(binding.name))) return .null;
        return switch (binding.view.column.column_type) {
            .string => if (input == .string) .{ .string = input.string } else error.InvalidRelationalExpressionInput,
            .integer => if (input == .integer) .{ .integer = input.integer } else error.InvalidRelationalExpressionInput,
            .number => if (input == .float) .{ .number = input.float } else if (input == .integer) .{ .number = @floatFromInt(input.integer) } else error.InvalidRelationalExpressionInput,
            .boolean => if (input == .bool) .{ .boolean = input.bool } else error.InvalidRelationalExpressionInput,
            .numeric => vm.numericJson(execution, input),
            .json => .{ .json = try roots.pinValue(input) },
            .enumeration => if (input == .string) .{ .enumeration = try binding.view.enumeration.?.input(input.string) } else error.InvalidRelationalExpressionInput,
            else => error.InvalidRelationalExpressionInput,
        };
    }

    fn requireWidths(self: *const Plan, cells: usize, present: usize, mask: usize) !void {
        if (cells != self.column_count or present != self.column_count or mask != self.column_count) return error.InvalidRelationalExpressionInput;
    }
};

fn preparedDigest(a: A, records: []const types.Record, columns: []const schema.RelationalColumn) ![32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:prepared-domain-contract:v1\x00");
    var word: [8]u8 = undefined;
    std.mem.writeInt(u64, &word, records.len, .little);
    hash.update(&word);
    for (records) |record| {
        const encoded = try record.encode(a);
        defer a.free(encoded);
        std.mem.writeInt(u64, &word, encoded.len, .little);
        hash.update(&word);
        hash.update(encoded);
    }
    std.mem.writeInt(u64, &word, columns.len, .little);
    hash.update(&word);
    for (columns, 0..) |column, ordinal| {
        const id = column.named_type_id orelse continue;
        std.mem.writeInt(u64, &word, ordinal, .little);
        hash.update(&word);
        std.mem.writeInt(u64, &word, id, .little);
        hash.update(&word);
        for ([_][]const u8{ column.name, column.path }) |text| {
            std.mem.writeInt(u64, &word, text.len, .little);
            hash.update(&word);
            hash.update(text);
        }
    }
    return hash.finalResult();
}

fn testRecord() !types.Record {
    return .{ .identity = try types.Identity.init(7, 2, 1, @splat(8)), .owner = .auth_disabled, .definition = .{ .domain = .{
        .base = .{ .builtin = .text },
        .not_null = true,
        .default_expression =
        \\{"op":"literal","type":"string","value":"queued"}
        ,
        .checks = &.{.{ .name = "state", .validated = false, .expression =
        \\{"op":"in_list","args":[{"op":"column","column":"value"},{"op":"literal","type":"string","value":"queued"},{"op":"literal","type":"string","value":"done"}]}
        }},
    } } };
}
fn testColumn() schema.RelationalColumn {
    return .{ .name = "status", .path = "status", .column_type = .string, .sql_element_type = .text, .named_type_id = 7, .allows_null = true };
}

test "SQL domain row preparation preserves explicit NULL defaults and untouched historical assignments" {
    const a = std.testing.allocator;
    const record = try testRecord();
    const table: schema.TableSchema = .{ .storage_mode = .relational, .named_types = &.{record}, .relational_columns = &.{testColumn()} };
    var plan = try Plan.init(a, table);
    defer plan.deinit();
    var bytes: usize = vm.max_allocated_bytes;
    var execution = vm.Execution.init(a, &bytes);
    var cells = [_]vm.Value{.null};
    var present = [_]bool{false};
    try plan.prepareDefaults(&execution, &cells, &present, &.{true});
    try std.testing.expect(present[0]);
    try std.testing.expectEqualStrings("queued", cells[0].string);
    try std.testing.expectEqual(@as(?Failure, null), try plan.validateAssigned(&execution, &cells, &present, &.{true}, null));
    cells[0] = .null;
    try plan.prepareDefaults(&execution, &cells, &present, &.{true});
    try std.testing.expect(cells[0] == .null);
    try std.testing.expectEqual(.not_null, (try plan.validateAssigned(&execution, &cells, &present, &.{true}, null)).?.violation.kind);
    cells[0] = .{ .string = "historical-invalid" };
    try std.testing.expectEqual(@as(?Failure, null), try plan.validateAssigned(&execution, &cells, &present, &.{false}, null));
    try std.testing.expectEqualStrings("state", (try plan.validateAssigned(&execution, &cells, &present, &.{true}, null)).?.violation.constraint.?);
    present[0] = false;
    try plan.prepareDefaults(&execution, &cells, &present, &.{false});
    try std.testing.expect(!present[0]);
    try std.testing.expectEqual(.not_null, (try plan.validateAssigned(&execution, &cells, &present, &.{true}, null)).?.violation.kind);
}

test "SQL domain row coercion trusts only exact owned SQL descriptors and rejects mismatched native bases" {
    const a = std.testing.allocator;
    const record = try testRecord();
    var column = testColumn();
    var plan = try Plan.init(a, .{ .storage_mode = .relational, .named_types = &.{record}, .relational_columns = &.{column} });
    defer plan.deinit();
    var bytes: usize = vm.max_allocated_bytes;
    var execution = vm.Execution.init(a, &bytes);
    var witness: ?domains.Reference = .{ .id = record.identity.id, .origin_id = record.identity.origin_id, .generation = record.identity.generation };
    try std.testing.expectEqual(@as(?Failure, null), try plan.validateAssigned(&execution, &.{.null}, &.{true}, &.{true}, &.{witness}));
    witness.?.generation += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, plan.validateAssigned(&execution, &.{.null}, &.{true}, &.{true}, &.{witness}));
    try std.testing.expectError(error.InvalidRelationalExpressionInput, plan.validateAssigned(&execution, &.{.null}, &.{true}, &.{true}, &.{}));
    column.sql_element_type = .uuid;
    try std.testing.expectError(error.InvalidSchema, Plan.init(a, .{ .storage_mode = .relational, .named_types = &.{record}, .relational_columns = &.{column} }));
}

test "SQL domain prepared row plans unwind every allocation failure without schema-width row allocations" {
    const a = std.testing.allocator;
    const record = try testRecord();
    const column = testColumn();
    const Harness = struct {
        fn run(alloc: A, source: types.Record, field: schema.RelationalColumn) !void {
            var plan = try Plan.init(alloc, .{ .storage_mode = .relational, .named_types = &.{source}, .relational_columns = &.{field} });
            defer plan.deinit();
            var bytes: usize = vm.max_allocated_bytes;
            var execution = vm.Execution.init(alloc, &bytes);
            var cells = [_]vm.Value{.null};
            var present = [_]bool{false};
            try plan.prepareDefaults(&execution, &cells, &present, &.{true});
            try std.testing.expectEqual(@as(?Failure, null), try plan.validateAssigned(&execution, &cells, &present, &.{true}, null));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Harness.run, .{ record, column });
    var plan = try Plan.init(a, .{ .storage_mode = .relational, .named_types = &.{record}, .relational_columns = &.{column} });
    defer plan.deinit();
    var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var bytes: usize = vm.max_allocated_bytes;
    var execution = vm.Execution.init(denied.allocator(), &bytes);
    var cells = [_]vm.Value{.null};
    var present = [_]bool{false};
    try plan.prepareDefaults(&execution, &cells, &present, &.{true});
    try std.testing.expectEqual(@as(?Failure, null), try plan.validateAssigned(&execution, &cells, &present, &.{true}, null));
    try std.testing.expect(!denied.has_induced_failure);
}

test "SQL domain sparse document preparation uses shared parsed input and exact epoch assignment provenance" {
    const a = std.testing.allocator;
    const record = try testRecord();
    var plan = try Plan.init(a, .{ .version = 3, .storage_mode = .relational, .named_types = &.{record}, .relational_columns = &.{testColumn()} });
    defer plan.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const region = arena.allocator();
    var bytes: usize = vm.max_allocated_bytes;
    var execution = vm.Execution.init(region, &bytes);
    const operands = @import("../sql/row_value.zig");
    var roots: operands.JsonRow = undefined;
    roots.initRegion(region, bytes, &execution.numeric);
    defer roots.deinit();
    try roots.bindAdmission(&bytes);
    var nulls: operands.JsonNulls = undefined;
    nulls.initRegion(region, bytes, &execution.numeric);
    defer nulls.deinit();
    try nulls.bindAdmission(&bytes);
    var document: std.json.Value = .{ .object = .empty };
    try plan.prepareJsonDefaults(&execution, &document, &nulls);
    try std.testing.expectEqualStrings("queued", document.object.get("status").?.string);
    try std.testing.expectEqual(@as(?Failure, null), try plan.validateJsonAssigned(&execution, document, &roots, &nulls, 3, null, null, .all));
    try document.object.put(region, "status", .{ .string = "historical-invalid" });
    try std.testing.expectEqual(@as(?Failure, null), try plan.validateJsonAssigned(&execution, document, &roots, &nulls, 3, &.{}, null, .all));
    try std.testing.expectEqualStrings("state", (try plan.validateJsonAssigned(&execution, document, &roots, &nulls, 3, &.{.{ .ordinal = 0 }}, null, .all)).?.violation.constraint.?);
    try document.object.put(region, "status", .null);
    const witness: domains.Reference = .{ .id = 7, .origin_id = record.identity.origin_id, .generation = 1 };
    try std.testing.expectEqual(@as(?Failure, null), try plan.validateJsonAssigned(&execution, document, &roots, &nulls, 3, &.{.{ .ordinal = 0, .source = witness }}, null, .all));
    try std.testing.expectError(error.CatalogGenerationChanged, plan.validateJsonAssigned(&execution, document, &roots, &nulls, 2, &.{.{ .ordinal = 0, .source = witness }}, null, .all));
    _ = document.object.orderedRemove("status");
    plan.excludeDomainDefaults(&.{"status"});
    try plan.prepareJsonDefaults(&execution, &document, &nulls);
    try std.testing.expect(document.object.get("status") == null);
}

fn testPublicSchema(a: A, extra: []const u8) ![]const u8 {
    const layout = try @import("../common/sql_named_type_layout.zig").toWireLeaky(a, try testRecord());
    const layouts = try std.json.Stringify.valueAlloc(a, [_]@TypeOf(layout){layout}, .{});
    return std.fmt.allocPrint(
        a,
        "{{\"version\":3,\"storage_mode\":\"relational\",\"enforce_types\":true,\"default_type\":\"row\",\"named_types\":{s},\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{{\"status\":{{\"type\":[\"text\",\"null\"],\"x-antfly-sql-type-id\":7}},\"note\":{{\"type\":[\"text\",\"null\"]}}}}}}}}}}{s}}}",
        .{ layouts, extra },
    );
}

test "SQL domain shared row admission prepares defaults and preserves historical provenance" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const r = arena.allocator();
    const validators = @import("mod.zig");
    var validator = try validators.CompiledTableValidator.init(a, try testPublicSchema(r, ""));
    defer validator.deinit(a);
    try std.testing.expect(validator.execution.domains != null);
    try std.testing.expect(!validator.execution.domains.?.owns_registry);
    try std.testing.expect(validator.execution.domains.?.bindings[0].view == validator.schema.document_schemas[0].properties[0].domain_view.?);
    // No expression staging arrays are needed for a domain-only table.
    try std.testing.expect(validator.execution.expressions == null);
    var row = try std.json.parseFromSliceLeaky(std.json.Value, r, "{}", .{});
    _ = try validator.prepareTypedValueWithNulls(r, r, &row, &.{}, false);
    try std.testing.expectEqualStrings("queued", row.object.get("status").?.string);
    row = try std.json.parseFromSliceLeaky(std.json.Value, r, "{\"status\":null}", .{});
    try std.testing.expectError(error.SqlNotNullViolation, validator.prepareTypedValueWithNulls(r, r, &row, &.{}, false));
    row = try std.json.parseFromSliceLeaky(std.json.Value, r, "{\"status\":\"historical\",\"note\":\"changed\"}", .{});
    try std.testing.expectError(error.RelationalCheckViolation, validator.prepareTypedValueWithNulls(r, r, &row, &.{}, false));
    // Restore preserves typed historical cells; NOT VALID is an assignment
    // rule, not a reason to reject a legitimate retained generation.
    _ = try validator.prepareTypedValueWithNulls(r, r, &row, &.{}, true);
    const patch = validator.forPreparation(.{ .io = std.testing.io, .domain_admission = .{ .schema_version = 3, .inputs = &.{}, .complete_row = false } });
    _ = try patch.prepareTypedValueWithNulls(r, r, &row, &.{}, false);
    try std.testing.expectEqualStrings("historical", row.object.get("status").?.string);
    _ = row.object.orderedRemove("status");
    _ = try patch.prepareTypedValueWithNulls(r, r, &row, &.{}, false);
    try std.testing.expect(!row.object.contains("status"));
    const trusted = validator.forPreparation(.{ .io = std.testing.io, .domain_admission = .{ .schema_version = 3, .inputs = &.{.{ .ordinal = 0, .source = .{ .id = 7, .origin_id = @splat(8), .generation = 1 } }} } });
    try row.object.put(r, "status", .null);
    _ = try trusted.prepareTypedValueWithNulls(r, r, &row, &.{}, false);
    const stale = validator.forPreparation(.{ .io = std.testing.io, .domain_admission = .{ .schema_version = 2, .inputs = &.{} } });
    try std.testing.expectError(error.CatalogGenerationChanged, stale.prepareTypedValueWithNulls(r, r, &row, &.{}, false));
}

test "SQL domain shared row defaults precede generated dependents and column defaults win" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const r = arena.allocator();
    const validators = @import("mod.zig");
    const extra =
        \\,"column_defaults":[{"column":"status","expression":{"op":"literal","type":"string","value":"done"}}],"generated_columns":[{"column":"note","expression":{"op":"column","column":"status"}}]
    ;
    var validator = try validators.CompiledTableValidator.init(a, try testPublicSchema(r, extra));
    defer validator.deinit(a);
    var row = try std.json.parseFromSliceLeaky(std.json.Value, r, "{}", .{});
    const candidate = try validator.prepareCandidateValueWithNulls(r, &row, &.{});
    try std.testing.expectEqualStrings("done", row.object.get("status").?.string);
    try std.testing.expect(!row.object.contains("note"));
    _ = try validator.prepareSelectedValueWithNulls(r, &row, candidate.fields);
    try std.testing.expectEqualStrings("done", row.object.get("note").?.string);
    row = try std.json.parseFromSliceLeaky(std.json.Value, r, "{\"status\":\"bad\"}", .{});
    try std.testing.expectError(error.RelationalCheckViolation, validator.prepareTypedValueWithNulls(r, r, &row, &.{}, false));
    try std.testing.expect(!row.object.contains("note"));
}

test "SQL domain shared row generated assignments cannot inherit input witnesses" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const r = arena.allocator();
    const validators = @import("mod.zig");
    const extra =
        \\,"generated_columns":[{"column":"status","expression":{"op":"column","column":"note"}}]
    ;
    var validator = try validators.CompiledTableValidator.init(a, try testPublicSchema(r, extra));
    defer validator.deinit(a);
    const trusted = validator.forPreparation(.{ .io = std.testing.io, .domain_admission = .{ .schema_version = 3, .inputs = &.{.{ .ordinal = 0, .source = .{ .id = 7, .origin_id = @splat(8), .generation = 1 } }}, .complete_row = false } });
    var row = try std.json.parseFromSliceLeaky(std.json.Value, r, "{\"status\":\"queued\",\"note\":\"invalid\"}", .{});
    try std.testing.expectError(error.RelationalCheckViolation, trusted.prepareTypedValueWithNulls(r, r, &row, &.{}, false));
    try row.object.put(r, "note", .{ .string = "done" });
    _ = try trusted.prepareTypedValueWithNulls(r, r, &row, &.{}, false);
    try std.testing.expectEqualStrings("done", row.object.get("status").?.string);
}

test "SQL domain complete assignments check omitted cells with sparse witnesses" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const r = arena.allocator();
    const validators = @import("mod.zig");
    const extra =
        \\,"column_defaults":[{"column":"status","expression":{"op":"literal","type":"string","value":null}}]
    ;
    var validator = try validators.CompiledTableValidator.init(a, try testPublicSchema(r, extra));
    defer validator.deinit(a);
    const insert = validator.forPreparation(.{ .io = std.testing.io, .domain_admission = .{ .schema_version = 3, .inputs = &.{} } });
    var row = try std.json.parseFromSliceLeaky(std.json.Value, r, "{}", .{});
    try std.testing.expectError(error.SqlNotNullViolation, insert.prepareTypedValueWithNulls(r, r, &row, &.{}, false));
    // Epoch checks also apply to complete raw input without any witnesses.
    const stale = validator.forPreparation(.{ .io = std.testing.io, .domain_admission = .{ .schema_version = 2 } });
    try std.testing.expectError(error.CatalogGenerationChanged, stale.prepareTypedValueWithNulls(r, r, &row, &.{}, false));
}

test "SQL domain shared row epoch compilation owns and unwinds all programs" {
    var region = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer region.deinit();
    const bytes = try testPublicSchema(region.allocator(), "");
    const Test = struct {
        fn run(a: A, declaration: []const u8) !void {
            var validator = try @import("mod.zig").CompiledTableValidator.init(a, declaration);
            defer validator.deinit(a);
            try std.testing.expect(validator.execution.domains != null);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Test.run, .{bytes});
}

test "SQL domain sparse DEFAULT requests preserve unrelated historical omissions" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const r = arena.allocator();
    const validators = @import("mod.zig");
    for ([_][]const u8{
        "",
        \\,"column_defaults":[{"column":"note","expression":{"op":"literal","type":"string","value":"fallback"}}]
        ,
        \\,"column_defaults":[{"column":"status","expression":{"op":"literal","type":"string","value":"done"}},{"column":"note","expression":{"op":"literal","type":"string","value":"fallback"}}]
        ,
    }, 0..) |extra, i| {
        var validator = try validators.CompiledTableValidator.init(a, try testPublicSchema(r, extra));
        defer validator.deinit(a);
        const resources: vm.PreparationRuntime = .{ .io = std.testing.io, .domain_admission = .{ .schema_version = 3, .inputs = &.{.{ .ordinal = 0, .use_default = true }}, .complete_row = false } };
        const update = validator.forPreparation(resources);
        var row = try std.json.parseFromSliceLeaky(std.json.Value, r, "{}", .{});
        _ = try update.prepareTypedValueWithNulls(r, r, &row, &.{}, false);
        try std.testing.expectEqualStrings(if (i < 2) "queued" else "done", row.object.get("status").?.string);
        try std.testing.expect(!row.object.contains("note"));
        _ = try update.prepareSelectedValueWithNulls(r, &row, &.{});
        try std.testing.expect(!row.object.contains("note"));
        // DEFAULT is a producer instruction, never a second conflicting value
        // or a way to inherit trusted source-domain provenance.
        try std.testing.expectError(error.InvalidRelationalExpressionInput, update.prepareTypedValueWithNulls(r, r, &row, &.{}, false));
        var forged = resources;
        forged.domain_admission.?.inputs = &.{.{ .ordinal = 0, .use_default = true, .source = .{ .id = 7, .generation = 1, .origin_id = @splat(8) } }};
        const invalid = validator.forPreparation(forged);
        row = try std.json.parseFromSliceLeaky(std.json.Value, r, "{}", .{});
        try std.testing.expectError(error.InvalidRelationalExpressionInput, invalid.prepareTypedValueWithNulls(r, r, &row, &.{}, false));
        try std.testing.expect(row.object.count() == 0);
    }
}

test "SQL domain shared row numeric checks preserve exact values below floating range" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const r = arena.allocator();
    const compiler = @import("../sql/compiler.zig");
    var compiled = try compiler.compile(a, "CREATE DOMAIN positive_amount AS numeric CHECK (VALUE > 0)", .{});
    defer compiled.deinit();
    var declaration = try @import("../sql/domain_definition.zig").Prepared.init(a, "positive_amount", compiled.statement.catalog_ddl.domain_definition.?.create);
    defer declaration.deinit();
    var record = try testRecord();
    record.definition = .{ .domain = declaration.definition };
    const layouts = @import("../common/sql_named_type_layout.zig");
    const layout_json = try std.json.Stringify.valueAlloc(r, try layouts.toWireLeaky(r, record), .{});
    var table = try std.json.parseFromSliceLeaky(std.json.Value, r, try testPublicSchema(r, ""), .{});
    table.object.getPtr("named_types").?.array.items[0] = try std.json.parseFromSliceLeaky(std.json.Value, r, layout_json, .{});
    const property = table.object.getPtr("document_schemas").?.object.getPtr("row").?.object.getPtr("schema").?.object.getPtr("properties").?.object.getPtr("status").?;
    property.object.getPtr("type").?.array.items[0] = .{ .string = "numeric" };
    var validator = try @import("mod.zig").CompiledTableValidator.init(a, try std.json.Stringify.valueAlloc(r, table, .{}));
    defer validator.deinit(a);
    for ([_][]const u8{ "1e-400", "-1e-400", "0", "null" }, 0..) |number, i| {
        const bytes = try std.fmt.allocPrint(r, "{{\"status\":{s}}}", .{number});
        var row = try std.json.parseFromSliceLeaky(std.json.Value, r, bytes, .{ .parse_numbers = false });
        if (i == 1 or i == 2) {
            try std.testing.expectError(error.RelationalCheckViolation, validator.prepareTypedValueWithNulls(r, r, &row, &.{}, false));
        } else {
            _ = try validator.prepareTypedValueWithNulls(r, r, &row, &.{}, false);
        }
    }
}

test "SQL domain candidate admission computes only the generated dependency closure before conflict discard" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const r = arena.allocator();
    const validators = @import("mod.zig");
    const independent =
        \\,"generated_columns":[{"column":"status","expression":{"op":"literal","type":"string","value":"done"}},{"column":"note","expression":{"op":"literal","type":"string","value":"deferred"}}]
    ;
    var validator = try validators.CompiledTableValidator.init(a, try testPublicSchema(r, independent));
    defer validator.deinit(a);
    var row = try std.json.parseFromSliceLeaky(std.json.Value, r, "{}", .{});
    const candidate = try validator.prepareCandidateValueWithNulls(r, &row, &.{});
    try std.testing.expectEqualStrings("done", row.object.get("status").?.string);
    try std.testing.expect(!row.object.contains("note"));
    _ = try validator.prepareSelectedValueWithNulls(r, &row, candidate.fields);
    try std.testing.expectEqualStrings("deferred", row.object.get("note").?.string);
    const dependent =
        \\,"generated_columns":[{"column":"status","expression":{"op":"column","column":"note"}},{"column":"note","expression":{"op":"literal","type":"string","value":"historical-invalid"}}]
    ;
    var rejected = try validators.CompiledTableValidator.init(a, try testPublicSchema(r, dependent));
    defer rejected.deinit(a);
    row = try std.json.parseFromSliceLeaky(std.json.Value, r, "{}", .{});
    try std.testing.expectError(error.RelationalCheckViolation, rejected.prepareCandidateValueWithNulls(r, &row, &.{}));
    try std.testing.expectEqualStrings("historical-invalid", row.object.get("note").?.string);
}

test "SQL domain canonical handoffs fence the destination and retain historical logical values without repairing generated outputs" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const r = arena.allocator();
    var validator = try @import("mod.zig").CompiledTableValidator.init(a, try testPublicSchema(r, ""));
    defer validator.deinit(a);
    const contract = validator.execution.domains.?.preparedContract();
    try validator.requirePreparedDomain(contract);
    const Write = @import("../storage/db/types.zig").BatchWrite;
    const Batch = @import("../storage/db/document_mapper.zig").PreparedDocumentBatch;
    try std.testing.expectError(error.SqlNotNullViolation, Batch.init(a, &[_]Write{.{ .key = "row", .value = "{\"status\":null}" }}, validator, false));
    var prepared = try Batch.init(a, &[_]Write{.{ .key = "row", .value = "{\"status\":null}", .prepared_domain = &contract }}, validator, false);
    defer prepared.deinit();
    try std.testing.expect(prepared.roots[0].?.object.get("status").? == .null);
    var historical = try Batch.init(a, &[_]Write{.{ .key = "row", .value = "{\"status\":\"historical\"}", .prepared_domain = &contract }}, validator, false);
    defer historical.deinit();
    try std.testing.expectEqualStrings("historical", historical.roots[0].?.object.get("status").?.string);
    var stale = contract;
    stale.schema_version += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, Batch.init(a, &[_]Write{.{ .key = "row", .value = "{\"status\":null}", .prepared_domain = &stale }}, validator, false));
    stale = contract;
    stale.domain_digest[0] ^= 1;
    try std.testing.expectError(error.CatalogGenerationChanged, validator.requirePreparedDomain(stale));
    const generated =
        \\,"generated_columns":[{"column":"note","expression":{"op":"column","column":"status"}}]
    ;
    var derived = try @import("mod.zig").CompiledTableValidator.init(a, try testPublicSchema(r, generated));
    defer derived.deinit(a);
    const derived_contract = derived.execution.domains.?.preparedContract();
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, Batch.init(a, &[_]Write{.{ .key = "row", .value = "{\"status\":null,\"note\":\"forged\"}", .prepared_domain = &derived_contract }}, derived, false));
}

test "SQL domain preparation contracts retain the legitimate initial schema epoch zero" {
    const contract: PreparedContract = .{ .schema_version = 0, .domain_digest = @splat(7) };
    try contract.validate();
    var wrong = contract;
    wrong.version = 2;
    try std.testing.expectError(error.InvalidRelationalExpressionInput, wrong.validate());
    wrong = contract;
    wrong.domain_digest = @splat(0);
    try std.testing.expectError(error.InvalidRelationalExpressionInput, wrong.validate());
}
