// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Trusted partition membership uses the ordinary versioned public-schema
//! persistence boundary. Generic schema requests cannot mint or discard it.
const std = @import("std");
const partitions = @import("../system_catalog/table_partitions.zig");
const A = std.mem.Allocator;
pub const field = "sql_partition";
pub const semantic_version = @import("../common/schema_readers.zig").sql_partition_semantic_version;
pub fn parseValue(a: A, value: std.json.Value) !std.json.Parsed(partitions.State) {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    var parsed = std.json.parseFromSlice(partitions.State, a, bytes, .{ .allocate = .alloc_always }) catch |failure| return if (failure == error.OutOfMemory) failure else error.InvalidPartitionDescriptor;
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}
pub fn canonicalize(a: A, root: *std.json.Value, state: partitions.State) !void {
    if (root.* != .object) return error.InvalidSchemaUpdateRequest;
    try state.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, state, .{});
    try root.object.put(a, field, try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{}));
}
pub fn rejectPublic(root: std.json.Value) !void {
    if (root != .object or root.object.contains(field)) return error.InvalidSchemaUpdateRequest;
}
pub fn preserve(a: A, previous: []const u8, input: []const u8) ![]u8 {
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const old = try std.json.parseFromSliceLeaky(std.json.Value, scratch, if (previous.len == 0) "{}" else previous, .{});
    var next = try std.json.parseFromSliceLeaky(std.json.Value, scratch, input, .{});
    try rejectPublic(next);
    if (old != .object) return error.InvalidSchemaUpdateRequest;
    if (old.object.get(field)) |state| {
        // Independent public schema evolution cannot strand the immutable
        // family directory at an unrelated leaf/parent definition. The family
        // DDL coordinator must publish propagated column/index successors.
        if (!std.mem.eql(u8, &try definitionDigest(scratch, previous), &try definitionDigest(scratch, input))) return error.SqlPartitionFamilyDdlRequired;
        var checked = try parseValue(scratch, state);
        defer checked.deinit();
        try canonicalize(scratch, &next, checked.value);
    }
    return std.json.Stringify.valueAlloc(a, next, .{});
}
pub fn stripUnchanged(a: A, previous: []const u8, input: []const u8) ![]u8 {
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const old = try std.json.parseFromSliceLeaky(std.json.Value, scratch, if (previous.len == 0) "{}" else previous, .{});
    var next = try std.json.parseFromSliceLeaky(std.json.Value, scratch, input, .{});
    if (old != .object or next != .object) return error.InvalidSchemaUpdateRequest;
    if (next.object.get(field)) |candidate| {
        const prior = old.object.get(field) orelse return error.InvalidSchemaUpdateRequest;
        var old_state = try parseValue(scratch, prior);
        defer old_state.deinit();
        var next_state = try parseValue(scratch, candidate);
        defer next_state.deinit();
        const old_bytes = try std.json.Stringify.valueAlloc(scratch, old_state.value, .{});
        const next_bytes = try std.json.Stringify.valueAlloc(scratch, next_state.value, .{});
        if (!std.mem.eql(u8, old_bytes, next_bytes)) return error.InvalidSchemaUpdateRequest;
        _ = next.object.orderedRemove(field);
    }
    return std.json.Stringify.valueAlloc(a, next, .{});
}

/// Called only after ordinary typed normalization/default/generated execution.
/// Reject parent physical writes, and enforce leaf bounds for every native API
/// path. SQL parent mutations must route before leaf preparation is requested.
pub fn validateJson(state: partitions.State, row: std.json.Value) !void {
    switch (state) {
        .parent => return error.SqlPartitionRoutingRequired,
        .member => |membership| {
            if (row != .object) return error.InvalidBatchRequest;
            const key = row.object.get(membership.key_name) orelse return error.SqlPartitionBoundViolation;
            if (!membership.bounds.contains(try timestampKey(key))) return error.SqlPartitionBoundViolation;
        },
    }
}

pub const timestampKey = partitions.timestampKey;

/// The complete immutable transition is carried as the shared owner selector.
/// It authorizes only partition metadata and an ordinary schema-version step;
/// indexes, defaults, checks, FK definitions and row layout remain untouched.
/// Additional inherited constraint/index installation is a separately verified
/// native input, never justified by this selector.
pub const Transition = struct {
    operation: enum { attach, detach, birth },
    inherited: []const partitions.ConstraintBinding = &.{},
    before: partitions.Descriptor,
    after: partitions.Descriptor,
    child_before: partitions.Relation,
    child_after: partitions.Relation,
    bounds: partitions.Range,

    pub fn validate(self: Transition, a: A) !void {
        try self.before.validate();
        try self.after.validate();
        try self.child_before.validate();
        try self.child_after.validate();
        try self.bounds.validate();
        if (self.before.parent.table_id != self.after.parent.table_id or
            self.child_before.table_id != self.child_after.table_id or
            self.child_before.table_id == self.before.parent.table_id or
            self.child_after.schema_version != (if (self.operation == .birth) self.child_before.schema_version else (std.math.add(u32, self.child_before.schema_version, 1) catch return error.InvalidPartitionDescriptor)) or
            self.after.parent.schema_version != (std.math.add(u32, self.before.parent.schema_version, 1) catch return error.InvalidPartitionDescriptor) or
            !std.mem.eql(u8, &self.child_before.definition, &self.child_after.definition) or
            !std.mem.eql(u8, &self.before.parent.definition, &self.after.parent.definition)) return error.InvalidPartitionDescriptor;
        var expected = switch (self.operation) {
            .attach, .birth => try self.before.attach(a, .{ .relation = self.child_after, .bounds = self.bounds, .inherited = self.inherited }),
            .detach => try self.before.detach(a, self.child_before.table_id),
        };
        defer a.free(expected.members);
        expected.parent.schema_version = self.after.parent.schema_version;
        if (!std.mem.eql(u8, &try expected.digest(), &try self.after.digest())) return error.InvalidPartitionDescriptor;
        if (self.operation == .detach) {
            const entry = for (self.before.members) |candidate| {
                if (candidate.relation.table_id == self.child_before.table_id) break candidate;
            } else return error.SqlNotPartition;
            if (!std.meta.eql(entry.relation, self.child_before) or !std.meta.eql(entry.bounds, self.bounds)) return error.PartitionCatalogChanged;
        }
    }
    pub fn fingerprint(self: Transition) !partitions.Digest {
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(std.crypto.hash.Blake3.init(.{}), &.{});
        try writer.writer.writeAll("antfly/partition-membership-transition/v1\x00");
        try std.json.Stringify.value(self, .{}, &writer.writer);
        var result: partitions.Digest = undefined;
        writer.hasher.final(&result);
        return result;
    }
    pub fn inverse(self: Transition, after: bool) !partitions.Membership {
        const parent = if (after) self.after else self.before;
        const entry = for (parent.members) |candidate| {
            if (candidate.relation.table_id == self.child_before.table_id) break candidate;
        } else return error.SqlNotPartition;
        return .{ .parent_id = parent.parent.table_id, .membership_generation = entry.membership_generation, .key_attribute = parent.key_attribute, .key_name = parent.key_name, .child = entry.relation, .bounds = entry.bounds, .parent_policy = parent.policy, .inherited = entry.inherited };
    }
    pub fn schemaAlloc(self: Transition, a: A, table: u64, previous: []const u8) ![]u8 {
        try self.validate(a);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var schema = try std.json.parseFromSliceLeaky(std.json.Value, scratch, previous, .{ .parse_numbers = false });
        if (schema != .object) return error.InvalidPartitionDescriptor;
        const relation = if (table == self.before.parent.table_id)
            self.before.parent
        else if (table == self.child_before.table_id)
            self.child_before
        else
            return error.InvalidPartitionDescriptor;
        const prior_definition = try digestDefinitionObject(schema.object);
        if (!std.mem.eql(u8, &prior_definition, &relation.definition)) return error.PartitionCatalogChanged;
        const prior_state = if (schema.object.get(field)) |raw| blk: {
            const state = try parseValue(scratch, raw);
            break :blk state.value;
        } else null;
        var next_version: u32 = undefined;
        if (table == self.before.parent.table_id) {
            if (prior_state == null or prior_state.? != .parent or !std.mem.eql(u8, &try prior_state.?.parent.digest(), &try self.before.digest())) return error.PartitionCatalogChanged;
            next_version = self.after.parent.schema_version;
            try canonicalize(scratch, &schema, .{ .parent = self.after });
        } else {
            if (self.operation == .attach or self.operation == .birth) {
                if (prior_state != null) return error.SqlAlreadyPartition;
                try canonicalize(scratch, &schema, .{ .member = try self.inverse(true) });
            } else {
                if (prior_state == null or prior_state.? != .member) return error.SqlNotPartition;
                try prior_state.?.member.require(self.before);
                _ = schema.object.orderedRemove(field);
            }
            next_version = self.child_after.schema_version;
        }
        const version = schema.object.get("version") orelse return error.InvalidPartitionDescriptor;
        const captured = switch (version) {
            .integer => |value| std.math.cast(u32, value) orelse return error.InvalidPartitionDescriptor,
            .number_string => |text| try std.fmt.parseInt(u32, text, 10),
            else => return error.InvalidPartitionDescriptor,
        };
        if (captured != (if (self.operation == .birth and table == self.child_after.table_id) next_version else next_version - 1)) return error.PartitionCatalogChanged;
        try schema.object.put(scratch, "version", .{ .integer = next_version });
        return std.json.Stringify.valueAlloc(a, schema, .{});
    }
};

/// Stable row/constraint declaration identity excludes the membership itself
/// and the changing schema epoch. Membership must never hash itself recursively.
pub fn definitionDigest(a: A, schema_json: []const u8) !partitions.Digest {
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const root = try std.json.parseFromSliceLeaky(std.json.Value, scratch, schema_json, .{ .parse_numbers = false });
    if (root != .object) return error.InvalidPartitionDescriptor;
    return digestDefinitionObject(root.object);
}

pub fn digestDefinitionObject(object: std.json.ObjectMap) !partitions.Digest {
    var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(std.crypto.hash.Blake3.init(.{}), &.{});
    try writer.writer.writeAll("antfly/partition-relation-definition/v1\x00");
    var json: std.json.Stringify = .{ .writer = &writer.writer };
    try json.beginObject();
    for (object.keys(), object.values()) |key, value| {
        if (std.mem.eql(u8, key, field) or std.mem.eql(u8, key, "version")) continue;
        try json.objectField(key);
        try json.write(value);
    }
    try json.endObject();
    var result: partitions.Digest = undefined;
    writer.hasher.final(&result);
    return result;
}

test "partition schema transition preserves declarations and public ingress cannot mint membership" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const source = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\"}";
    const identity: partitions.Relation = .{ .table_id = 1, .schema_version = 1, .definition = try definitionDigest(a, source) };
    var child = identity;
    child.table_id = 2;
    var child_after = child;
    child_after.schema_version = 2;
    const parent: partitions.Descriptor = .{ .parent = identity, .revision = 1, .key_attribute = 1, .key_name = "created_at" };
    const bounds: partitions.Range = .{ .lower = .{ .value = 0 }, .upper = .{ .value = 100 } };
    var after = try parent.attach(a, .{ .relation = child_after, .bounds = bounds });
    defer a.free(after.members);
    after.parent.schema_version = 2;
    const transition: Transition = .{ .operation = .attach, .before = parent, .after = after, .child_before = child, .child_after = child_after, .bounds = bounds };
    var root = try std.json.parseFromSliceLeaky(std.json.Value, scratch, source, .{});
    try canonicalize(scratch, &root, .{ .parent = parent });
    const parent_json = try std.json.Stringify.valueAlloc(scratch, root, .{});
    const preserved = try preserve(a, parent_json, source);
    defer a.free(preserved);
    try std.testing.expect(std.mem.indexOf(u8, preserved, field) != null);
    try std.testing.expectError(error.SqlPartitionFamilyDdlRequired, preserve(a, parent_json, "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"changed\"}"));
    const parent_next = try transition.schemaAlloc(a, 1, parent_json);
    defer a.free(parent_next);
    const unrelated = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"changed\"}";
    try std.testing.expectError(error.PartitionCatalogChanged, transition.schemaAlloc(a, 2, unrelated));
    const child_next = try transition.schemaAlloc(a, 2, source);
    defer a.free(child_next);
    try std.testing.expectEqual(try definitionDigest(a, source), try definitionDigest(a, parent_next));
    try std.testing.expectEqual(try definitionDigest(a, source), try definitionDigest(a, child_next));
    var decoded = try std.json.parseFromSlice(std.json.Value, a, child_next, .{});
    defer decoded.deinit();
    var membership = try parseValue(a, decoded.value.object.get(field).?);
    defer membership.deinit();
    try membership.value.member.require(after);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, preserve(a, source, child_next));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, stripUnchanged(a, source, child_next));
    try std.testing.expectError(error.PartitionCatalogChanged, transition.schemaAlloc(a, 1, parent_next));
    var detached = try after.detach(a, 2);
    defer a.free(detached.members);
    detached.parent.schema_version = 3;
    var child_detached = child_after;
    child_detached.schema_version = 3;
    const detach: Transition = .{ .operation = .detach, .before = after, .after = detached, .child_before = child_after, .child_after = child_detached, .bounds = bounds };
    const independent = try detach.schemaAlloc(a, 2, child_next);
    defer a.free(independent);
    const value = try std.json.parseFromSliceLeaky(std.json.Value, scratch, independent, .{});
    try std.testing.expect(!value.object.contains(field));
}

test "partition native write validator consumes exact unsigned nanoseconds and rejects NULL" {
    const a = std.testing.allocator;
    const timestamp = try @import("../sql/temporal/text.zig").parseInput(.timestamp_tz, "2026-01-01");
    const bound = timestamp.timestamp_tz.micros;
    const state: partitions.State = .{ .member = .{ .parent_id = 1, .membership_generation = 2, .key_attribute = 1, .key_name = "created_at", .child = .{ .table_id = 2, .schema_version = 1, .definition = @splat(1) }, .bounds = .{ .lower = .{ .value = bound }, .upper = .{ .value = bound + 1 } } } };
    const text = try std.fmt.allocPrint(a, "{{\"created_at\":{d}}}", .{timestamp.timestamp_tz.unixNanoseconds().?});
    defer a.free(text);
    var row = try std.json.parseFromSlice(std.json.Value, a, text, .{ .parse_numbers = false });
    defer row.deinit();
    try validateJson(state, row.value);
    var missing = try std.json.parseFromSlice(std.json.Value, a, "{\"created_at\":null}", .{});
    defer missing.deinit();
    try std.testing.expectError(error.SqlPartitionBoundViolation, validateJson(state, missing.value));
    const later_text = try std.fmt.allocPrint(a, "{{\"created_at\":{d}}}", .{timestamp.timestamp_tz.unixNanoseconds().? + 1000});
    defer a.free(later_text);
    var later = try std.json.parseFromSlice(std.json.Value, a, later_text, .{ .parse_numbers = false });
    defer later.deinit();
    try std.testing.expectError(error.SqlPartitionBoundViolation, validateJson(state, later.value));
}

pub fn validateLayout(a: A, schema: anytype, runtime: anytype) !void {
    const state = (schema.sql_partition orelse return).value;
    if (schema.storage_mode != .relational or runtime.storage_mode != .relational or schema.external_base_source != null)
        return error.InvalidPartitionDescriptor;
    const declared_version = switch (state) {
        .parent => |parent| parent.parent.schema_version,
        .member => |member| member.child.schema_version,
    };
    if (declared_version != schema.version or runtime.version != schema.version) return error.InvalidPartitionDescriptor;
    const key_name = switch (state) {
        .parent => |parent| parent.key_name,
        .member => |member| member.key_name,
    };
    if (isGenerated(schema, key_name)) return error.UnsupportedSqlShape;
    const key = for (runtime.relational_columns) |column| {
        if (std.mem.eql(u8, column.name, key_name)) break column;
    } else return error.InvalidPartitionDescriptor;
    if (key.column_type != .datetime or key.named_type_id != null) return error.InvalidPartitionDescriptor;
    if (state == .parent) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const uniques = try schema.relationalUniqueDefinitions(arena.allocator());
        for (uniques) |unique| try partitions.requireUniqueKey(&.{key_name}, unique.columns);
    }
}

pub fn isGenerated(schema: anytype, name: []const u8) bool {
    const definitions = schema.generated_columns orelse return false;
    if (definitions.value != .array) return false;
    for (definitions.value.array.items) |definition| {
        if (definition != .object) continue;
        const column = definition.object.get("column") orelse continue;
        if (column == .string and std.mem.eql(u8, column.string, name)) return true;
    }
    return false;
}

/// Names borrow an immutable validated schema; the caller owns the directory.
pub fn generatedNames(a: A, schema: anytype) !std.StringHashMapUnmanaged(void) {
    var names: std.StringHashMapUnmanaged(void) = .empty;
    errdefer names.deinit(a);
    if (schema.generated_columns) |definitions| {
        if (definitions.value == .null) return names;
        if (definitions.value != .array) return error.InvalidSchema;
        for (definitions.value.array.items) |definition| {
            if (definition != .object) return error.InvalidSchema;
            const column = definition.object.get("column") orelse return error.InvalidSchema;
            if (column != .string) return error.InvalidSchema;
            try names.put(a, column.string, {});
        }
    }
    return names;
}

/// The native catalog caller supplies the allocated table and attribute identities.
pub fn createParentSchema(a: A, table_id: u64, key_attribute: u64, schema_json: []const u8, intent: partitions.Create) ![]u8 {
    try intent.validate();
    if (key_attribute == 0) return error.InvalidPartitionDescriptor;
    const api = @import("mod.zig");
    var definition = try api.PreparedSchemaDefinition.init(a, schema_json);
    defer definition.deinit(a);
    if (definition.public_schema.sql_partition != null or definition.runtime_schema.storage_mode != .relational or definition.runtime_schema.external_base_source != null) return error.InvalidPartitionDescriptor;
    const key = for (definition.runtime_schema.relational_columns) |column| {
        if (std.mem.eql(u8, column.name, intent.key_name)) break column;
    } else return error.UndefinedColumn;
    if (key.column_type != .datetime or key.named_type_id != null) return error.UnsupportedSqlShape;
    if (isGenerated(definition.public_schema, intent.key_name)) return error.UnsupportedSqlShape;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const uniques = try definition.public_schema.relationalUniqueDefinitions(scratch);
    for (uniques) |unique| try partitions.requireUniqueKey(&.{intent.key_name}, unique.columns);
    var root = try std.json.parseFromSliceLeaky(std.json.Value, scratch, schema_json, .{ .parse_numbers = false });
    const descriptor: partitions.Descriptor = .{ .parent = .{ .table_id = table_id, .schema_version = definition.runtime_schema.version, .definition = try definitionDigest(a, schema_json) }, .revision = 1, .key_attribute = key_attribute, .key_name = intent.key_name };
    try canonicalize(scratch, &root, .{ .parent = descriptor });
    return std.json.Stringify.valueAlloc(a, root, .{});
}

/// Native row imports/restores do not traverse JSON preparation. Enforce the
/// same captured membership against the authenticated immutable AROW layout.
pub fn validateRow(state: partitions.State, row: anytype) !void {
    switch (state) {
        .parent => return error.SqlPartitionRoutingRequired,
        .member => |membership| {
            const ordinal = row.ordinalForName(membership.key_name) orelse return error.SqlPartitionBoundViolation;
            const cell = (try row.findCell(ordinal)) orelse return error.SqlPartitionBoundViolation;
            if (cell.is_null) return error.SqlPartitionBoundViolation;
            if (cell.value_type != .u64_val) return error.RelationalRowSchemaMismatch;
            const timestamp = try @import("../sql/temporal/value.zig").Timestamp.fromUnixNanoseconds(cell.value.u64_val);
            if (!membership.bounds.contains(timestamp.micros)) return error.SqlPartitionBoundViolation;
        },
    }
}

test "partition birth initializes final inverse without inventing a previous child schema epoch" {
    const a = std.testing.allocator;
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const source = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\"}";
    const parent: partitions.Descriptor = .{ .parent = .{ .table_id = 1, .schema_version = 1, .definition = try definitionDigest(a, source) }, .revision = 4, .key_attribute = 9, .key_name = "created_at" };
    const child: partitions.Relation = .{ .table_id = 2, .schema_version = 1, .definition = try definitionDigest(a, source) };
    const bounds: partitions.Range = .{ .lower = .minvalue, .upper = .maxvalue };
    var after = try parent.attach(a, .{ .relation = child, .bounds = bounds });
    defer a.free(after.members);
    after.parent.schema_version = 2;
    const birth: Transition = .{ .operation = .birth, .before = parent, .after = after, .child_before = child, .child_after = child, .bounds = bounds };
    try birth.validate(a);
    var parent_root = try std.json.parseFromSliceLeaky(std.json.Value, scratch, source, .{});
    try canonicalize(scratch, &parent_root, .{ .parent = parent });
    const parent_source = try std.json.Stringify.valueAlloc(scratch, parent_root, .{});
    const parent_next = try birth.schemaAlloc(a, 1, parent_source);
    defer a.free(parent_next);
    const child_initial = try birth.schemaAlloc(a, 2, source);
    defer a.free(child_initial);
    const child_root = try std.json.parseFromSliceLeaky(std.json.Value, scratch, child_initial, .{});
    try std.testing.expectEqual(@as(i64, 1), child_root.object.get("version").?.integer);
    var inverse = try parseValue(a, child_root.object.get(field).?);
    defer inverse.deinit();
    try inverse.value.member.require(after);
    try std.testing.expectEqualDeep(child, inverse.value.member.child);
    try std.testing.expectEqual(try definitionDigest(a, source), try definitionDigest(a, child_initial));
    // Ordinary attach still requires an existing child's successor epoch;
    // neither form may be substituted into the other's immutable selector.
    var forged = birth;
    forged.operation = .attach;
    try std.testing.expectError(error.InvalidPartitionDescriptor, forged.validate(a));
    forged = birth;
    forged.child_after.schema_version = 2;
    try std.testing.expectError(error.InvalidPartitionDescriptor, forged.validate(a));
    try std.testing.expectError(error.SqlAlreadyPartition, birth.schemaAlloc(a, 2, child_initial));
}
