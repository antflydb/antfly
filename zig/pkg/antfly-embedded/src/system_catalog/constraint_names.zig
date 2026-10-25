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

//! PostgreSQL-compatible generated constraint names.
//! The admission owner supplies a captured namespace occupancy view. This
//! module does not scan a catalog, publish a name, or rename explicit names.
const std = @import("std");

pub const max_name_bytes = 63;
// Match the native relation-name registry's source-identifier bound. SQL
// generated names have the smaller PostgreSQL bound above. Reject oversized
// inputs before UTF-8 validation rather than rescanning arbitrary input for
// every constraint in a large DDL statement.
pub const max_source_name_bytes = 256;

/// Internal naming policy, not a public schema resource kind or wire enum.
pub const Kind = enum {
    primary,
    unique,
    check,
    foreign_key,

    pub fn includesRelationNames(self: Kind) bool {
        return self == .primary or self == .unique;
    }
};

/// Borrows the table name; owns a bounded prefix of the joined key names.
/// Initialize once per constraint, not once per collision probe.
pub const Candidate = struct {
    table: []const u8,
    kind: Kind,
    sequence_label: bool = false,
    addition: [max_name_bytes * 2 + 1]u8 = undefined,
    addition_len: usize = 0,

    pub fn init(table: []const u8, columns: []const []const u8, primary: bool) !Candidate {
        return initKind(table, columns, if (primary) .primary else .unique);
    }

    /// Owned sequences use the same PostgreSQL relation-name clipping and
    /// collision suffix policy, while the caller supplies relation occupancy.
    pub fn initSequence(table: []const u8, column: []const u8) !Candidate {
        var candidate = try initKind(table, &.{column}, .unique);
        candidate.sequence_label = true;
        return candidate;
    }

    /// CHECK takes referenced column names, possibly repeated. PostgreSQL
    /// includes a column only when the expression refers to exactly one
    /// distinct column; zero/multiple columns produce table_check.
    pub fn initKind(table: []const u8, columns: []const []const u8, kind: Kind) !Candidate {
        try validate(table);
        var self: Candidate = .{ .table = table, .kind = kind };
        if (kind == .primary) return self;
        if (kind == .check) {
            if (columns.len == 0) return self;
            try validate(columns[0]);
            for (columns[1..]) |column| {
                try validate(column);
                if (!std.mem.eql(u8, columns[0], column)) return self;
            }
            const part = clip(columns[0], max_name_bytes);
            @memcpy(self.addition[0..part.len], part);
            self.addition_len = part.len;
            return self;
        }
        if (columns.len == 0) return error.InvalidCatalogName;
        // Only a bounded prefix can contribute to a 63-byte name. Joining
        // every key would allocate proportional to the complete schema.
        for (columns) |column| {
            try validate(column);
            if (self.addition_len != 0) {
                self.addition[self.addition_len] = '_';
                self.addition_len += 1;
            }
            const part = clip(column, max_name_bytes);
            @memcpy(self.addition[self.addition_len..][0..part.len], part);
            self.addition_len += part.len;
            if (self.addition_len >= max_name_bytes + 1) break;
        }
        return self;
    }

    /// Zero selects the unsuffixed name; later attempts preserve the complete
    /// numeric label and shorten table/key prefixes at UTF-8 boundaries.
    pub fn format(self: *const Candidate, attempt: u32, output: *[max_name_bytes]u8) ![]const u8 {
        var label_buffer: [15]u8 = undefined;
        const stem = if (self.sequence_label) "seq" else switch (self.kind) {
            .primary => "pkey",
            .unique => "key",
            .check => "check",
            .foreign_key => "fkey",
        };
        const label = if (attempt == 0) stem else try std.fmt.bufPrint(&label_buffer, "{s}{d}", .{ stem, attempt });
        const right = self.addition[0..self.addition_len];
        const overhead = label.len + 1 + @intFromBool(right.len != 0);
        const available = max_name_bytes - overhead;
        var left_len = self.table.len;
        var right_len = right.len;
        if (left_len > available or right_len > available - left_len) {
            // Equivalent to repeatedly shortening the longer prefix, with
            // ties shortening the right prefix, but constant-time in length.
            const left_half = (available + 1) / 2;
            if (left_len < left_half) {
                right_len = available - left_len;
            } else if (right_len < available / 2) {
                left_len = available - right_len;
            } else {
                left_len = left_half;
                right_len = available - left_half;
            }
        }
        const left_part = clip(self.table, left_len);
        const right_part = clip(right, right_len);
        var length: usize = 0;
        @memcpy(output[length..][0..left_part.len], left_part);
        length += left_part.len;
        if (right.len != 0) {
            output[length] = '_';
            length += 1;
            @memcpy(output[length..][0..right_part.len], right_part);
            length += right_part.len;
        }
        output[length] = '_';
        length += 1;
        @memcpy(output[length..][0..label.len], label);
        return output[0 .. length + label.len];
    }
};

fn validate(name: []const u8) !void {
    if (name.len == 0 or name.len > max_source_name_bytes or std.mem.indexOfScalar(u8, name, 0) != null or !std.unicode.utf8ValidateSlice(name))
        return error.InvalidCatalogName;
}

fn clip(bytes: []const u8, limit: usize) []const u8 {
    var end = @min(bytes.len, limit);
    while (end != 0 and end < bytes.len and bytes[end] & 0xc0 == 0x80) end -= 1;
    return bytes[0..end];
}

/// PostgreSQL names anonymous UNIQUE constraints after all index attributes,
/// including INCLUDE payloads. Repeated source columns get distinct physical
/// attribute labels before the final constraint name is bounded. This is
/// naming only: it never changes the schema's source column references.
fn coveringNames(a: std.mem.Allocator, keys: []const []const u8, includes: std.json.Value) ![]const []const u8 {
    if (includes == .null) return keys;
    if (includes != .array or includes.array.items.len > 256) return error.InvalidCatalogRecord;
    if (includes.array.items.len == 0) return keys;
    const output = try a.alloc([]const u8, keys.len + includes.array.items.len);
    var names: std.StringHashMapUnmanaged(void) = .empty;
    defer names.deinit(a);
    try names.ensureTotalCapacity(a, @intCast(output.len));
    for (output, 0..) |*out, index| {
        const source = if (index < keys.len) keys[index] else blk: {
            const value = includes.array.items[index - keys.len];
            if (value != .string) return error.InvalidCatalogRecord;
            break :blk value.string;
        };
        try validate(source);
        var buffer: [max_name_bytes]u8 = undefined;
        var candidate = clip(source, max_name_bytes);
        var attempt: usize = 0;
        while (names.contains(candidate)) {
            attempt += 1;
            if (attempt > output.len) return error.ProgramLimitExceeded;
            var suffix_buffer: [20]u8 = undefined;
            const suffix = try std.fmt.bufPrint(&suffix_buffer, "{d}", .{attempt});
            const prefix = clip(source, max_name_bytes - suffix.len);
            @memcpy(buffer[0..prefix.len], prefix);
            @memcpy(buffer[prefix.len..][0..suffix.len], suffix);
            candidate = buffer[0 .. prefix.len + suffix.len];
        }
        out.* = try a.dupe(u8, candidate);
        names.putAssumeCapacity(out.*, {});
    }
    return output;
}

/// The occupancy callback receives the naming kind: PRIMARY/UNIQUE check
/// relation and constraint names; CHECK/FK check constraint memberships only.
/// It must include names already visible at this statement's PostgreSQL DDL
/// phase, not blindly reserve every future explicit name. It must also check
/// cancellation/deadlines during admission.
/// Allocate only the chosen name. Exhaustion is definite nonadmission, not a
/// reason to overwrite a conflicting relation or replay an uncertain write.
pub fn choose(alloc: std.mem.Allocator, candidate: *const Candidate, namespace: anytype, max_attempts: u32) ![]const u8 {
    var output: [max_name_bytes]u8 = undefined;
    for (0..max_attempts) |attempt| {
        const name = try candidate.format(@intCast(attempt), &output);
        if (!try namespace.occupied(name, candidate.kind)) return alloc.dupe(u8, name);
    }
    return error.ProgramLimitExceeded;
}

pub const Selected = struct { name: []const u8, kind: Kind };

/// Selected-name predicates are rechecked against the current covered root in
/// the caller's atomic transaction, not against an unrelated catalog revision.
pub const Admission = struct {
    namespace_id: u64,
    names: []const Selected,

    pub fn dupe(self: Admission, a: std.mem.Allocator) !Admission {
        const result = try a.alloc(Selected, self.names.len);
        var initialized: usize = 0;
        errdefer {
            for (result[0..initialized]) |entry| a.free(entry.name);
            a.free(result);
        }
        for (self.names, result) |entry, *copy| {
            copy.* = .{ .name = try a.dupe(u8, entry.name), .kind = entry.kind };
            initialized += 1;
        }
        return .{ .namespace_id = self.namespace_id, .names = result };
    }

    pub fn deinitOwned(self: *Admission, a: std.mem.Allocator) void {
        for (self.names) |entry| a.free(entry.name);
        a.free(self.names);
        self.* = undefined;
    }

    /// Match each predicate once to its actual final schema declaration.
    /// Hash lookup keeps validation linear, rather than rescanning the schema
    /// once for every selected name. Explicit declarations need no predicate.
    pub fn validateSchema(self: Admission, a: std.mem.Allocator, schema_json: []const u8) !void {
        if (self.namespace_id == 0 or self.names.len == 0 or self.names.len > 8192 or schema_json.len > 16 * 1024 * 1024) return error.InvalidCatalogRecord;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var selected: std.StringHashMapUnmanaged(struct { kind: Kind, seen: bool = false }) = .empty;
        for (self.names) |entry| {
            try validate(entry.name);
            if (entry.name.len > max_name_bytes) return error.InvalidCatalogRecord;
            const slot = try selected.getOrPut(scratch, entry.name);
            if (slot.found_existing) return error.InvalidCatalogRecord;
            slot.value_ptr.* = .{ .kind = entry.kind };
        }
        const schema = try std.json.parseFromSliceLeaky(std.json.Value, scratch, schema_json, .{ .parse_numbers = false });
        if (schema != .object) return error.InvalidCatalogRecord;
        for ([_][]const u8{ "checks", "unique_constraints", "foreign_keys" }, [_]Kind{ .check, .unique, .foreign_key }) |key, base_kind| {
            const rules = schema.object.get(key) orelse continue;
            if (rules != .array or rules.array.items.len > 8192) return error.InvalidCatalogRecord;
            for (rules.array.items) |rule| {
                if (rule != .object) return error.InvalidCatalogRecord;
                const label = rule.object.get("name") orelse return error.InvalidCatalogRecord;
                if (label != .string) return error.InvalidCatalogRecord;
                if (base_kind == .unique) if (rule.object.get("origin")) |origin| {
                    if (origin != .null) {
                        if (origin != .string) return error.InvalidCatalogRecord;
                        // A standalone UNIQUE index is not a constraint. A
                        // CHECK/FK can legitimately have the same SQL name.
                        if (std.mem.eql(u8, origin.string, "index")) continue;
                        if (!std.mem.eql(u8, origin.string, "constraint")) return error.InvalidCatalogRecord;
                    }
                };
                if (selected.getPtr(label.string)) |expected| {
                    if (expected.seen) return error.InvalidCatalogRecord;
                    var kind = base_kind;
                    if (base_kind == .unique) if (rule.object.get("primary")) |primary| {
                        if (primary != .bool) return error.InvalidCatalogRecord;
                        if (primary.bool) kind = .primary;
                    };
                    if (kind != expected.kind) return error.InvalidCatalogRecord;
                    expected.seen = true;
                }
            }
        }
        var values = selected.valueIterator();
        while (values.next()) |value| if (!value.seen) return error.InvalidCatalogRecord;
    }
};

pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    schema_json: []const u8,
    admission: Admission,

    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Input is compiler-normalized CREATE schema and trusted generated-name
    /// provenance, never a user-editable flag embedded in schema JSON. The
    /// reader pins one covered namespace cut and implements checkpoint() plus
    /// occupied(name,kind) and relationOccupied(name). Explicit index names
    /// check only relations, not another table's nonexclusive constraint name.
    /// Cancellation applies even to local-only collisions.
    pub fn prepare(alloc: std.mem.Allocator, table: []const u8, namespace_id: u64, schema_json: []const u8, generated: []const []const u8, reader: anytype, max_attempts: u32) !Plan {
        return prepareWithBaseline(alloc, table, namespace_id, null, schema_json, generated, reader, max_attempts);
    }

    /// Prepare one additive ALTER constraint against a captured table schema.
    /// The fragment contains only that constraint, not a replacement schema.
    /// Existing rules are seeded as local occupancy, never re-admitted as new
    /// declarations. The caller must fence the captured owner/schema at commit.
    pub fn prepareAddition(alloc: std.mem.Allocator, table: []const u8, namespace_id: u64, baseline_schema_json: []const u8, fragment_json: []const u8, generated: []const []const u8, reader: anytype, max_attempts: u32) !Plan {
        if (generated.len != 1) return error.InvalidCatalogRecord;
        if (baseline_schema_json.len > 16 * 1024 * 1024) return error.ProgramLimitExceeded;
        return prepareWithBaseline(alloc, table, namespace_id, baseline_schema_json, fragment_json, generated, reader, max_attempts);
    }

    fn prepareWithBaseline(alloc: std.mem.Allocator, table: []const u8, namespace_id: u64, baseline_schema_json: ?[]const u8, schema_json: []const u8, generated: []const []const u8, reader: anytype, max_attempts: u32) !Plan {
        if (namespace_id == 0 or generated.len == 0) return error.InvalidCatalogRecord;
        if (generated.len > 8192 or schema_json.len > 16 * 1024 * 1024) return error.ProgramLimitExceeded;
        _ = try Candidate.initKind(table, &.{}, .primary);
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        const Stage = struct {
            a: std.mem.Allocator,
            table: []const u8,
            reader: @TypeOf(reader),
            hints: std.StringHashMapUnmanaged(usize) = .empty,
            constraints: std.StringHashMapUnmanaged(void) = .empty,
            relations: std.StringHashMapUnmanaged(void) = .empty,
            selected: []Selected,
            seen: []bool,
            max_attempts: u32,
            probes_left: u32,
            nodes_left: usize = 8192,

            pub fn occupied(self: *@This(), name: []const u8, kind: Kind) !bool {
                try self.reader.checkpoint();
                if (self.probes_left == 0) return error.ProgramLimitExceeded;
                self.probes_left -= 1;
                return self.constraints.contains(name) or (kind.includesRelationNames() and self.relations.contains(name)) or
                    try self.reader.occupied(name, kind);
            }

            fn rules(schema: *std.json.Value, key: []const u8) ![]std.json.Value {
                if (schema.* != .object) return error.InvalidCatalogRecord;
                const list = schema.object.getPtr(key) orelse return &.{};
                if (list.* != .array) return error.InvalidCatalogRecord;
                if (list.array.items.len > 8192) return error.ProgramLimitExceeded;
                return list.array.items;
            }

            fn primary(rule: std.json.Value) !bool {
                if (rule != .object) return error.InvalidCatalogRecord;
                const flag = rule.object.get("primary") orelse return false;
                if (flag != .bool) return error.InvalidCatalogRecord;
                return flag.bool;
            }

            fn seed(self: *@This(), baseline: *std.json.Value) !void {
                for ([_][]const u8{ "checks", "unique_constraints", "foreign_keys" }) |key| {
                    for (try rules(baseline, key)) |rule| {
                        try self.reader.checkpoint();
                        if (rule != .object) return error.InvalidCatalogRecord;
                        const name = rule.object.get("name") orelse return error.InvalidCatalogRecord;
                        if (name != .string) return error.InvalidCatalogRecord;
                        try validate(name.string);
                        const unique = std.mem.eql(u8, key, "unique_constraints");
                        var index_owned = false;
                        if (unique) if (rule.object.get("origin")) |origin| {
                            if (origin != .null) {
                                if (origin != .string) return error.InvalidCatalogRecord;
                                index_owned = std.mem.eql(u8, origin.string, "index");
                                if (!index_owned and !std.mem.eql(u8, origin.string, "constraint")) return error.InvalidCatalogRecord;
                            }
                        };
                        if (!index_owned) {
                            const entry = try self.constraints.getOrPut(self.a, name.string);
                            if (entry.found_existing) return error.InvalidCatalogRecord;
                        }
                        if (unique) try self.relations.put(self.a, name.string, {});
                    }
                }
                for (try rules(baseline, "relational_indexes")) |rule| {
                    try self.reader.checkpoint();
                    if (rule != .object) return error.InvalidCatalogRecord;
                    const name = rule.object.get("name") orelse return error.InvalidCatalogRecord;
                    if (name != .string) return error.InvalidCatalogRecord;
                    try validate(name.string);
                    try self.relations.put(self.a, name.string, {});
                }
            }

            fn column(self: *@This(), expression: std.json.Value, depth: usize, single: *?[]const u8, multiple: *bool) !void {
                try self.reader.checkpoint();
                if (depth > 64 or self.nodes_left == 0) return error.ProgramLimitExceeded;
                if (expression != .object) return error.InvalidCatalogRecord;
                self.nodes_left -= 1;
                const op = expression.object.get("op") orelse return error.InvalidCatalogRecord;
                if (op != .string) return error.InvalidCatalogRecord;
                if (std.mem.eql(u8, op.string, "literal")) return; // JSON literals are not expression nodes.
                if (std.mem.eql(u8, op.string, "column")) {
                    const label = expression.object.get("column") orelse return error.InvalidCatalogRecord;
                    if (label != .string) return error.InvalidCatalogRecord;
                    try validate(label.string);
                    if (single.*) |previous| {
                        if (!std.mem.eql(u8, previous, label.string)) multiple.* = true;
                    } else single.* = label.string;
                }
                if (expression.object.get("args")) |args| {
                    if (args != .array) return error.InvalidCatalogRecord;
                    for (args.array.items) |arg| try self.column(arg, depth + 1, single, multiple);
                }
            }

            fn apply(self: *@This(), rule: *std.json.Value, kind: Kind) !void {
                try self.reader.checkpoint();
                if (rule.* != .object) return error.InvalidCatalogRecord;
                const label = rule.object.getPtr("name") orelse return error.InvalidCatalogRecord;
                if (label.* != .string) return error.InvalidCatalogRecord;
                try validate(label.string);
                if (self.hints.get(label.string)) |index| {
                    if (self.seen[index]) return error.InvalidCatalogRecord;
                    if (kind.includesRelationNames()) if (rule.object.get("origin")) |origin| {
                        if (origin != .null and (origin != .string or !std.mem.eql(u8, origin.string, "constraint"))) return error.InvalidCatalogRecord;
                    };
                    var columns: []const []const u8 = &.{};
                    var check_column: [1][]const u8 = undefined;
                    if (kind == .check) {
                        const expression = rule.object.get("expression") orelse return error.InvalidCatalogRecord;
                        var single: ?[]const u8 = null;
                        var multiple = false;
                        try self.column(expression, 0, &single, &multiple);
                        if (!multiple) if (single) |value| {
                            check_column[0] = value;
                            columns = &check_column;
                        };
                    } else if (kind != .primary) {
                        const keys = rule.object.get(if (kind == .foreign_key) "child_columns" else "columns") orelse return error.InvalidCatalogRecord;
                        if (keys != .array or keys.array.items.len == 0 or keys.array.items.len > 256) return error.InvalidCatalogRecord;
                        const values = try self.a.alloc([]const u8, keys.array.items.len);
                        for (keys.array.items, values) |key, *value| {
                            if (key != .string) return error.InvalidCatalogRecord;
                            value.* = key.string;
                        }
                        columns = values;
                        if (kind == .unique) if (rule.object.get("include_columns")) |includes| {
                            columns = try coveringNames(self.a, columns, includes);
                        };
                    }
                    const candidate = try Candidate.initKind(self.table, columns, kind);
                    const chosen = try choose(self.a, &candidate, self, self.max_attempts);
                    label.* = .{ .string = chosen };
                    self.selected[index] = .{ .name = chosen, .kind = kind };
                    self.seen[index] = true;
                } else if (kind.includesRelationNames() and try self.reader.relationOccupied(label.string)) return error.SqlIndexAlreadyExists;
                if (kind.includesRelationNames() and self.relations.contains(label.string)) return error.SqlIndexAlreadyExists;
                if (self.constraints.contains(label.string)) return error.SqlConstraintAlreadyExists;
                try self.constraints.put(self.a, label.string, {});
                if (kind.includesRelationNames()) try self.relations.put(self.a, label.string, {});
            }
        };
        var stage: Stage = .{
            .a = a,
            .table = table,
            .reader = reader,
            .selected = try a.alloc(Selected, generated.len),
            .seen = try a.alloc(bool, generated.len),
            .max_attempts = max_attempts,
            .probes_left = max_attempts,
        };
        @memset(stage.seen, false);
        for (generated, 0..) |source, index| {
            try validate(source);
            const entry = try stage.hints.getOrPut(a, source);
            if (entry.found_existing) return error.InvalidCatalogRecord;
            entry.value_ptr.* = index;
        }
        try stage.relations.put(a, table, {});
        var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, schema_json, .{ .parse_numbers = false });
        var baseline: ?std.json.Value = null;
        if (baseline_schema_json) |bytes| {
            baseline = try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{ .parse_numbers = false });
            try stage.seed(&baseline.?);
            if (schema != .object) return error.InvalidCatalogRecord;
            var rule_count: usize = 0;
            for (schema.object.keys()) |key| {
                if (!std.mem.eql(u8, key, "checks") and !std.mem.eql(u8, key, "unique_constraints") and !std.mem.eql(u8, key, "foreign_keys")) return error.InvalidCatalogRecord;
                rule_count += (try Stage.rules(&schema, key)).len;
            }
            if (rule_count != 1) return error.InvalidCatalogRecord;
        }
        for (try Stage.rules(&schema, "checks")) |*rule| try stage.apply(rule, .check);
        const uniques = try Stage.rules(&schema, "unique_constraints");
        for ([_]bool{ true, false }) |is_primary| for (uniques) |*rule| {
            if (try Stage.primary(rule.*) == is_primary) try stage.apply(rule, if (is_primary) .primary else .unique);
        };
        for (try Stage.rules(&schema, "foreign_keys")) |*rule| try stage.apply(rule, .foreign_key);
        for (stage.seen) |seen| if (!seen) return error.InvalidCatalogRecord;
        if (baseline) |*base| {
            for ([_][]const u8{ "checks", "unique_constraints", "foreign_keys" }) |key| {
                const additions = try Stage.rules(&schema, key);
                if (additions.len == 0) continue;
                const entry = try base.object.getOrPut(a, key);
                if (!entry.found_existing) entry.value_ptr.* = .{ .array = std.json.Array.init(a) };
                try entry.value_ptr.array.appendSlice(additions);
            }
            schema = base.*;
        }
        // Finish allocating before copying arena state into the owned result.
        // Serialization can grow a new arena block; copying first loses it.
        const encoded = try std.json.Stringify.valueAlloc(a, schema, .{});
        return .{ .arena = arena, .schema_json = encoded, .admission = .{ .namespace_id = namespace_id, .names = stage.selected } };
    }
};

fn repeated(comptime bytes: []const u8, comptime count: usize) [bytes.len * count]u8 {
    var result: [bytes.len * count]u8 = undefined;
    for (0..count) |i| @memcpy(result[i * bytes.len ..][0..bytes.len], bytes);
    return result;
}

const EmptyNamespace = struct {
    cancelled: bool = false,
    pub fn checkpoint(self: *@This()) !void {
        if (self.cancelled) return error.DeadlineExceeded;
    }
    pub fn occupied(_: *@This(), _: []const u8, _: Kind) !bool {
        return false;
    }
    pub fn relationOccupied(_: *@This(), _: []const u8) !bool {
        return false;
    }
};

const plan_schema =
    \\{"checks":[{"name":"t_email_key","expression":{"op":"literal","value":true}}],"unique_constraints":[{"name":"anonymous_unique","columns":["email"]},{"name":"anonymous_primary","columns":["id"],"primary":true}],"foreign_keys":[{"name":"anonymous_fk","child_columns":["id"]}]}
;

fn preparedPlanAllocationCase(a: std.mem.Allocator) !void {
    var namespace: EmptyNamespace = .{};
    var plan = try Plan.prepare(a, "t", 5, plan_schema, &.{ "anonymous_unique", "anonymous_primary", "anonymous_fk" }, &namespace, 16);
    defer plan.deinit();
    try std.testing.expectEqualStrings("t_email_key1", plan.admission.names[0].name);
    try std.testing.expectEqualStrings("t_pkey", plan.admission.names[1].name);
    try std.testing.expectEqualStrings("t_id_fkey", plan.admission.names[2].name);
    try plan.admission.validateSchema(a, plan.schema_json);
    const copy = try plan.admission.dupe(a);
    defer {
        for (copy.names) |entry| a.free(entry.name);
        a.free(copy.names);
    }
    try copy.validateSchema(a, plan.schema_json);
}

test "prepared constraint naming uses staged CHECK index FK visibility and owns its predicates" {
    try preparedPlanAllocationCase(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, preparedPlanAllocationCase, .{});
}

fn preparedAdditionAllocationCase(a: std.mem.Allocator) !void {
    var namespace: EmptyNamespace = .{};
    const baseline = "{\"version\":7,\"custom\":{\"preserved\":true},\"checks\":[{\"name\":\"t_id_check\",\"expression\":{\"op\":\"literal\",\"value\":true}}],\"unique_constraints\":[{\"name\":\"old_pk\",\"columns\":[\"id\"],\"primary\":true}]}";
    const fragment = "{\"checks\":[{\"name\":\"anonymous\",\"expression\":{\"op\":\"column\",\"column\":\"id\"}}]}";
    var plan = try Plan.prepareAddition(a, "t", 5, baseline, fragment, &.{"anonymous"}, &namespace, 16);
    defer plan.deinit();
    try std.testing.expectEqualStrings("t_id_check1", plan.admission.names[0].name);
    try plan.admission.validateSchema(a, plan.schema_json);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, plan.schema_json, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 7), parsed.value.object.get("version").?.integer);
    try std.testing.expect(parsed.value.object.get("custom").?.object.get("preserved").?.bool);
    const rules = parsed.value.object.get("checks").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), rules.len);
    try std.testing.expectEqualStrings("t_id_check", rules[0].object.get("name").?.string);
    try std.testing.expectEqualStrings("old_pk", parsed.value.object.get("unique_constraints").?.array.items[0].object.get("name").?.string);
}

test "prepared ALTER addition preserves baseline ownership and is allocation safe" {
    try preparedAdditionAllocationCase(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, preparedAdditionAllocationCase, .{});
}

test "prepared ALTER distinguishes standalone index and existing constraint occupancy" {
    const a = std.testing.allocator;
    const cases = [_]struct { baseline: []const u8, fragment: []const u8, expected: []const u8 }{
        .{ .baseline = "{\"unique_constraints\":[{\"name\":\"t_id_check\",\"columns\":[\"id\"],\"origin\":\"index\"}],\"relational_indexes\":[{\"name\":\"t_id_check\"}]}", .fragment = "{\"checks\":[{\"name\":\"anonymous\",\"expression\":{\"op\":\"column\",\"column\":\"id\"}}]}", .expected = "t_id_check" },
        .{ .baseline = "{\"relational_indexes\":[{\"name\":\"t_id_key\"}]}", .fragment = "{\"unique_constraints\":[{\"name\":\"anonymous\",\"columns\":[\"id\"]}]}", .expected = "t_id_key1" },
        .{ .baseline = "{\"checks\":[{\"name\":\"t_id_fkey\"}]}", .fragment = "{\"foreign_keys\":[{\"name\":\"anonymous\",\"child_columns\":[\"id\"]}]}", .expected = "t_id_fkey1" },
    };
    for (cases) |case| {
        var namespace: EmptyNamespace = .{};
        var plan = try Plan.prepareAddition(a, "t", 5, case.baseline, case.fragment, &.{"anonymous"}, &namespace, 16);
        defer plan.deinit();
        try std.testing.expectEqualStrings(case.expected, plan.admission.names[0].name);
        try plan.admission.validateSchema(a, plan.schema_json);
    }
}

fn coveringNameAllocationCase(a: std.mem.Allocator) !void {
    for ([_]struct { includes: []const u8, expected: []const u8 }{
        .{ .includes = "[\"label\"]", .expected = "t_id_label_key" },
        .{ .includes = "[\"id\"]", .expected = "t_id_id1_key" },
        .{ .includes = "[\"id\",\"id\",\"label\",\"label\"]", .expected = "t_id_id1_id2_label_label1_key" },
    }) |case| {
        const fragment = try std.fmt.allocPrint(a, "{{\"unique_constraints\":[{{\"name\":\"anonymous\",\"columns\":[\"id\"],\"include_columns\":{s}}}]}}", .{case.includes});
        defer a.free(fragment);
        var namespace: EmptyNamespace = .{};
        var plan = try Plan.prepareAddition(a, "t", 5, "{}", fragment, &.{"anonymous"}, &namespace, 16);
        defer plan.deinit();
        try std.testing.expectEqualStrings(case.expected, plan.admission.names[0].name);
        try plan.admission.validateSchema(a, plan.schema_json);
    }
}

test "prepared covering UNIQUE names include payload attributes and disambiguate repeated columns" {
    try coveringNameAllocationCase(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, coveringNameAllocationCase, .{});
}

test "prepared ALTER addition rejects replacement and multi-rule fragments" {
    const a = std.testing.allocator;
    var namespace: EmptyNamespace = .{};
    const fragment = "{\"checks\":[{\"name\":\"anonymous\",\"expression\":{\"op\":\"literal\",\"value\":true}}]}";
    try std.testing.expectError(error.InvalidCatalogRecord, Plan.prepareAddition(a, "t", 5, "{}", fragment, &.{ "anonymous", "other" }, &namespace, 16));
    try std.testing.expectError(error.InvalidCatalogRecord, Plan.prepareAddition(a, "t", 5, "{}", "{\"version\":2,\"checks\":[]}", &.{"anonymous"}, &namespace, 16));
    try std.testing.expectError(error.InvalidCatalogRecord, Plan.prepareAddition(a, "t", 5, "{}", "{\"checks\":[{\"name\":\"anonymous\"},{\"name\":\"explicit\"}]}", &.{"anonymous"}, &namespace, 16));
    namespace.cancelled = true;
    try std.testing.expectError(error.DeadlineExceeded, Plan.prepareAddition(a, "t", 5, "{\"checks\":[{\"name\":\"old\"}]}", fragment, &.{"anonymous"}, &namespace, 16));
}

test "prepared naming rejects future explicit collisions and bounds aggregate probes" {
    var namespace: EmptyNamespace = .{};
    const unique = "{\"unique_constraints\":[{\"name\":\"anonymous\",\"columns\":[\"id\"],\"primary\":true},{\"name\":\"t_pkey\",\"columns\":[\"email\"]}]}";
    try std.testing.expectError(error.SqlIndexAlreadyExists, Plan.prepare(std.testing.allocator, "t", 5, unique, &.{"anonymous"}, &namespace, 16));
    const check_after = "{\"checks\":[{\"name\":\"anonymous\",\"expression\":{\"op\":\"column\",\"column\":\"id\"}},{\"name\":\"t_id_check\",\"expression\":{\"op\":\"literal\",\"value\":true}}]}";
    try std.testing.expectError(error.SqlConstraintAlreadyExists, Plan.prepare(std.testing.allocator, "t", 5, check_after, &.{"anonymous"}, &namespace, 16));
    const checks = "{\"checks\":[{\"name\":\"first\",\"expression\":{\"op\":\"literal\",\"value\":true}},{\"name\":\"second\",\"expression\":{\"op\":\"literal\",\"value\":false}}]}";
    try std.testing.expectError(error.ProgramLimitExceeded, Plan.prepare(std.testing.allocator, "t", 5, checks, &.{ "first", "second" }, &namespace, 2));
    try std.testing.expectError(error.InvalidCatalogRecord, Plan.prepare(std.testing.allocator, "t", 5, checks, &.{"missing"}, &namespace, 16));
    try std.testing.expectError(error.InvalidCatalogRecord, Plan.prepare(std.testing.allocator, "t", 5, checks, &.{ "first", "first" }, &namespace, 16));
    namespace.cancelled = true;
    try std.testing.expectError(error.DeadlineExceeded, Plan.prepare(std.testing.allocator, "t", 5, checks, &.{"first"}, &namespace, 16));
}

test "selected constraint predicates authenticate final schema kinds and uniqueness" {
    const names = [_]Selected{.{ .name = "chosen", .kind = .check }};
    const admission: Admission = .{ .namespace_id = 5, .names = &names };
    try admission.validateSchema(std.testing.allocator, "{\"checks\":[{\"name\":\"chosen\"}]}");
    try std.testing.expectError(error.InvalidCatalogRecord, admission.validateSchema(std.testing.allocator, "{\"unique_constraints\":[{\"name\":\"chosen\"}]}"));
    try std.testing.expectError(error.InvalidCatalogRecord, admission.validateSchema(std.testing.allocator, "{\"checks\":[{\"name\":\"chosen\"},{\"name\":\"chosen\"}]}"));
    try std.testing.expectError(error.InvalidCatalogRecord, admission.validateSchema(std.testing.allocator, "{\"checks\":[{\"name\":\"other\"}]}"));
    const unique: Admission = .{ .namespace_id = 5, .names = &.{.{ .name = "chosen", .kind = .unique }} };
    try std.testing.expectError(error.InvalidCatalogRecord, unique.validateSchema(std.testing.allocator, "{\"unique_constraints\":[{\"name\":\"chosen\",\"origin\":\"index\"}]}"));
}

test "prepared CHECK names collect distinct expression columns without walking JSON literal data" {
    const cases = [_]struct { expression: []const u8, expected: []const u8 }{
        .{ .expression = "{\"op\":\"add\",\"args\":[{\"op\":\"column\",\"column\":\"id\"},{\"op\":\"column\",\"column\":\"id\"}]}", .expected = "t_id_check" },
        .{ .expression = "{\"op\":\"gt\",\"args\":[{\"op\":\"column\",\"column\":\"id\"},{\"op\":\"column\",\"column\":\"tenant\"}]}", .expected = "t_check" },
        .{ .expression = "{\"op\":\"is_not_null\",\"args\":[{\"op\":\"literal\",\"value\":{\"op\":\"column\",\"column\":\"not_a_reference\"}}]}", .expected = "t_check" },
    };
    for (cases) |case| {
        const schema = try std.fmt.allocPrint(std.testing.allocator, "{{\"checks\":[{{\"name\":\"anonymous\",\"expression\":{s}}}]}}", .{case.expression});
        defer std.testing.allocator.free(schema);
        var namespace: EmptyNamespace = .{};
        var plan = try Plan.prepare(std.testing.allocator, "t", 5, schema, &.{"anonymous"}, &namespace, 16);
        defer plan.deinit();
        try std.testing.expectEqualStrings(case.expected, plan.admission.names[0].name);
        try plan.admission.validateSchema(std.testing.allocator, plan.schema_json);
    }
}

test "generated unique constraint names preserve PostgreSQL prefixes suffixes and UTF8 clipping" {
    var output: [max_name_bytes]u8 = undefined;
    const unique = try Candidate.init("items", &.{ "tenant", "email" }, false);
    try std.testing.expectEqualStrings("items_tenant_email_key", try unique.format(0, &output));
    try std.testing.expectEqualStrings("items_tenant_email_key3", try unique.format(3, &output));
    const primary = try Candidate.init("items", &.{}, true);
    try std.testing.expectEqualStrings("items_pkey", try primary.format(0, &output));
    try std.testing.expectEqualStrings("items_pkey4294967295", try primary.format(std.math.maxInt(u32), &output));
    const left = repeated("a", 63);
    const right = repeated("b", 63);
    const long = try Candidate.init(&left, &.{&right}, false);
    try std.testing.expectEqualStrings(comptime std.fmt.comptimePrint("{s}_{s}_key", .{ repeated("a", 29), repeated("b", 29) }), try long.format(0, &output));
    try std.testing.expectEqualStrings(comptime std.fmt.comptimePrint("{s}_{s}_key1", .{ repeated("a", 29), repeated("b", 28) }), try long.format(1, &output));
    const unicode_left = repeated("é", 31);
    const unicode_right = repeated("中", 21);
    const unicode = try Candidate.init(&unicode_left, &.{&unicode_right}, false);
    try std.testing.expectEqualStrings(comptime std.fmt.comptimePrint("{s}_{s}_key", .{ repeated("é", 14), repeated("中", 9) }), try unicode.format(0, &output));
    try std.testing.expect(std.unicode.utf8ValidateSlice(try unicode.format(100, &output)));
    const long_primary = try Candidate.init(&left, &.{}, true);
    try std.testing.expectEqualStrings(comptime std.fmt.comptimePrint("{s}_pkey", .{repeated("a", 58)}), try long_primary.format(0, &output));
    const first_column = repeated("a", 40);
    const second_column = repeated("b", 40);
    const composite = try Candidate.init("items", &.{ &first_column, &second_column }, false);
    try std.testing.expectEqualStrings(comptime std.fmt.comptimePrint("items_{s}_{s}_key", .{ repeated("a", 40), repeated("b", 12) }), try composite.format(0, &output));
    try std.testing.expectError(error.InvalidCatalogName, Candidate.init("", &.{"id"}, false));
    try std.testing.expectError(error.InvalidCatalogName, Candidate.init("items", &.{}, false));
    try std.testing.expectError(error.InvalidCatalogName, Candidate.init("items", &.{"\xff"}, false));
    const oversized = repeated("x", max_source_name_bytes + 1);
    try std.testing.expectError(error.InvalidCatalogName, Candidate.init(&oversized, &.{"id"}, true));
    try std.testing.expectError(error.InvalidCatalogName, Candidate.init("items", &.{&oversized}, false));
}

test "generated CHECK and FK names use PostgreSQL distinct-column and complete suffix policies" {
    var output: [max_name_bytes]u8 = undefined;
    const cases = [_]struct { kind: Kind, columns: []const []const u8, expected: []const u8 }{
        .{ .kind = .check, .columns = &.{}, .expected = "items_check" },
        .{ .kind = .check, .columns = &.{"id"}, .expected = "items_id_check" },
        .{ .kind = .check, .columns = &.{ "id", "id", "id" }, .expected = "items_id_check" },
        .{ .kind = .check, .columns = &.{ "id", "tenant" }, .expected = "items_check" },
        .{ .kind = .foreign_key, .columns = &.{"id"}, .expected = "items_id_fkey" },
        .{ .kind = .foreign_key, .columns = &.{ "tenant", "id" }, .expected = "items_tenant_id_fkey" },
    };
    for (cases) |case| {
        const candidate = try Candidate.initKind("items", case.columns, case.kind);
        try std.testing.expectEqualStrings(case.expected, try candidate.format(0, &output));
    }
    const check = try Candidate.initKind("items", &.{"id"}, .check);
    try std.testing.expectEqualStrings("items_id_check4294967295", try check.format(std.math.maxInt(u32), &output));
    const left = repeated("a", 63);
    const right = repeated("b", 63);
    const long_check = try Candidate.initKind(&left, &.{&right}, .check);
    try std.testing.expectEqualStrings(comptime std.fmt.comptimePrint("{s}_{s}_check", .{ repeated("a", 28), repeated("b", 28) }), try long_check.format(0, &output));
    const long_fk = try Candidate.initKind(&left, &.{&right}, .foreign_key);
    try std.testing.expectEqualStrings(comptime std.fmt.comptimePrint("{s}_{s}_fkey", .{ repeated("a", 29), repeated("b", 28) }), try long_fk.format(0, &output));
    try std.testing.expectError(error.InvalidCatalogName, Candidate.initKind("items", &.{}, .foreign_key));
}

const Namespace = struct {
    calls: usize = 0,
    fail: bool = false,
    pub fn occupied(self: *Namespace, name: []const u8, _: Kind) !bool {
        self.calls += 1;
        if (self.fail) return error.DeadlineExceeded;
        return std.mem.eql(u8, name, "items_email_key") or std.mem.eql(u8, name, "items_email_key1");
    }
};

fn allocationCase(alloc: std.mem.Allocator) !void {
    const candidate = try Candidate.init("items", &.{"email"}, false);
    var namespace: Namespace = .{};
    const name = try choose(alloc, &candidate, &namespace, 3);
    defer alloc.free(name);
    try std.testing.expectEqualStrings("items_email_key2", name);
    try std.testing.expectEqual(@as(usize, 3), namespace.calls);
}

test "generated unique constraint name selection is bounded cancellable and allocation safe" {
    try allocationCase(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationCase, .{});
    const candidate = try Candidate.init("items", &.{"email"}, false);
    var namespace: Namespace = .{};
    try std.testing.expectError(error.ProgramLimitExceeded, choose(std.testing.allocator, &candidate, &namespace, 2));
    try std.testing.expectEqual(@as(usize, 2), namespace.calls);
    namespace.fail = true;
    try std.testing.expectError(error.DeadlineExceeded, choose(std.testing.allocator, &candidate, &namespace, 3));
}

fn chooseKindsAllocationCase(alloc: std.mem.Allocator) !void {
    const Occupancy = struct {
        first: []const u8,
        relation: bool,
        membership: bool,
        calls: usize = 0,
        pub fn occupied(self: *@This(), label: []const u8, kind: Kind) !bool {
            self.calls += 1;
            return std.mem.eql(u8, label, self.first) and
                (self.membership or (self.relation and kind.includesRelationNames()));
        }
    };
    for ([_]Kind{ .primary, .unique, .check, .foreign_key }) |kind| {
        const candidate = try Candidate.initKind("items", &.{"id"}, kind);
        var first_buffer: [max_name_bytes]u8 = undefined;
        const first = try candidate.format(0, &first_buffer);
        for ([_]bool{ false, true }) |membership| {
            var occupancy: Occupancy = .{ .first = first, .relation = true, .membership = membership };
            const result = try choose(alloc, &candidate, &occupancy, 2);
            defer alloc.free(result);
            const collided = membership or kind.includesRelationNames();
            var expected_buffer: [max_name_bytes]u8 = undefined;
            try std.testing.expectEqualStrings(try candidate.format(@intFromBool(collided), &expected_buffer), result);
            try std.testing.expectEqual(@as(usize, if (collided) 2 else 1), occupancy.calls);
        }
    }
}

test "generated constraint selection distinguishes relation collisions from constraint membership" {
    try chooseKindsAllocationCase(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, chooseKindsAllocationCase, .{});
}
