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

//! Immutable validation-only publication preparation. No admission, root
//! authentication, topology authority or publication is granted by this module.
//! The accepting metadata writer must independently compare the exact catalog,
//! complete live owner cover, physical preparation and decoder/reader barriers.
const std = @import("std");
const local = @import("antfly_local_sources");
const contract = local.storage_db_relational_schema_owner_contract;
const records = local.common_topology_records;
const tables = @import("table_manager.zig");
const barriers = @import("schema_validation_publication.zig");
const api = @import("../api/tables.zig");
const A = std.mem.Allocator;
const Digest = [32]u8;
pub const max_bytes = 16 * 1024 * 1024;

pub const OwnerIntent = struct {
    range: records.RangeRecord,
    binding: contract.Binding,
    physical_root_incarnation: u128,
};

pub const Plan = struct {
    version: u16 = 1,
    metadata_group_id: u64,
    metadata_incarnation: local.metadata_incarnation.MetadataClusterIncarnation,
    id: [16]u8,
    table_guard: ?local.system_catalog_domain.RelationMutationGuard = null,
    before: records.TableRecord,
    after: records.TableRecord,
    columns: []const []const u8,
    owners: []const OwnerIntent,

    fn requireBoundedShape(self: Plan) !void {
        if (self.version != 1 or self.metadata_group_id == 0 or
            !local.metadata_incarnation.isValid(self.metadata_incarnation) or std.mem.allEqual(u8, &self.id, 0) or
            self.owners.len == 0 or self.owners.len > barriers.max_owners or
            self.columns.len == 0 or self.columns.len > contract.max_columns or
            self.before.schema_json.len > contract.max_schema or self.after.schema_json.len > contract.max_schema or
            self.before.indexes_json.len > contract.max_schema or self.after.indexes_json.len > contract.max_schema or
            self.before.storage_migration != null or self.after.storage_migration != null)
            return error.InvalidGenerationPublication;
        var raw_bytes: usize = 0;
        for ([_]records.TableRecord{ self.before, self.after }) |table| {
            inline for (@typeInfo(records.TableRecord).@"struct".field_names) |field| {
                if (comptime @TypeOf(@field(table, field)) == []const u8) {
                    raw_bytes = std.math.add(usize, raw_bytes, @field(table, field).len) catch return error.InvalidGenerationPublication;
                    if (raw_bytes > max_bytes) return error.InvalidGenerationPublication;
                }
            }
            if (table.full_text_read_index) |name| if (name.len == 0 or name.len > 1024) return error.InvalidGenerationPublication;
        }
        var projection = contract.ProjectionHasher.init();
        for (self.columns) |column| try projection.add(column);
        for (self.owners) |owner| {
            inline for (@typeInfo(records.RangeRecord).@"struct".field_names) |field| {
                if (comptime @TypeOf(@field(owner.range, field)) == []const u8) {
                    raw_bytes = std.math.add(usize, raw_bytes, @field(owner.range, field).len) catch return error.InvalidGenerationPublication;
                    if (raw_bytes > max_bytes) return error.InvalidGenerationPublication;
                }
            }
            if (owner.range.end_key) |end| {
                raw_bytes = std.math.add(usize, raw_bytes, end.len) catch return error.InvalidGenerationPublication;
                if (raw_bytes > max_bytes) return error.InvalidGenerationPublication;
            }
        }
        if (self.table_guard) |guard| {
            try guard.validate();
            if (guard.constraint_name_admission != null or guard.owner.kind != .table or guard.owner.table_id != self.before.table_id or
                guard.owner.schema_version != try api.schemaVersion(self.before.schema_json) or
                !std.mem.eql(u8, &guard.owner.schema_digest, &contract.hash(self.before.schema_json)) or
                !std.mem.eql(u8, guard.target.name, guard.logical_table)) return error.InvalidGenerationPublication;
        }
    }

    /// The plan commits every owner intent and its preparation root. Omit only
    /// the resulting binding digest itself, avoiding a circular commitment.
    /// This is not a signature and cannot authenticate supplied preparation.
    pub fn digest(self: Plan) !Digest {
        try self.requireBoundedShape();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/schema-validation-immutable-plan/v1");
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(hash, &.{});
        try std.json.Stringify.value(.{
            .version = self.version,
            .metadata_group_id = self.metadata_group_id,
            .metadata_incarnation = self.metadata_incarnation,
            .id = self.id,
            .table_guard = self.table_guard,
            .before = self.before,
            .after = self.after,
            .columns = self.columns,
            .owner_count = self.owners.len,
        }, .{}, &writer.writer);
        for (self.owners) |owner| {
            var binding = owner.binding;
            binding.plan_digest = @splat(0);
            try writer.writer.writeByte(0);
            try std.json.Stringify.value(.{ .range = owner.range, .binding = binding, .physical_root_incarnation = owner.physical_root_incarnation }, .{}, &writer.writer);
        }
        var result: Digest = undefined;
        writer.hasher.final(&result);
        return result;
    }

    pub fn validate(self: Plan, alloc: A) !barriers.Head {
        try self.requireBoundedShape();
        var projection = contract.ProjectionHasher.init();
        for (self.columns) |column| try projection.add(column);
        const projection_digest = try projection.finish();
        const replacement = try api.prepareNullabilityPublicationRecord(alloc, &self.before, self.columns);
        defer tables.freeTable(alloc, replacement);
        if (!tables.tableDefinitionsEqual(replacement, self.after)) return error.InvalidGenerationPublication;
        var before = try local.schema_mod.PreparedSchemaDefinition.init(alloc, self.before.schema_json);
        defer before.deinit(alloc);
        var after = try local.schema_mod.PreparedSchemaDefinition.init(alloc, self.after.schema_json);
        defer after.deinit(alloc);
        const native_before = try local.storage_schema.serializeSchema(alloc, before.runtime_schema);
        defer alloc.free(native_before);
        const native_after = try local.storage_schema.serializeSchema(alloc, after.runtime_schema);
        defer alloc.free(native_after);
        const before_schema_hash = contract.hash(native_before);
        const after_schema_hash = contract.hash(native_after);
        const before_public_hash = contract.hash(self.before.schema_json);
        const after_public_hash = contract.hash(self.after.schema_json);
        const commitment = try self.digest();
        var reader_floor: local.common_schema_readers.ReaderFloor = .{};
        if (self.before.schema_reader_requirements) |required| reader_floor = try reader_floor.include(required);
        if (self.after.schema_reader_requirements) |required| reader_floor = try reader_floor.include(required);
        var head: barriers.Head = .{
            .metadata_group_id = self.metadata_group_id,
            .metadata_incarnation = self.metadata_incarnation,
            .id = self.id,
            .plan_digest = commitment,
            .table_id = self.before.table_id,
            .owner_count = @intCast(self.owners.len),
            .reader_requirements = if (reader_floor.active()) .{ .semantic_version = reader_floor.semantic_version, .encoding_version = reader_floor.encoding_version } else null,
        };
        try head.validate();
        // The short admission/recovery workspace is off the ACK path. Never
        // materialize all immutable schema/owner inputs for a single ACK.
        const owners = try alloc.alloc(barriers.Owner, self.owners.len);
        defer alloc.free(owners);
        const ranges = try alloc.alloc(records.RangeRecord, self.owners.len);
        defer alloc.free(ranges);
        var input_ordinals: std.AutoHashMapUnmanaged(Digest, u16) = .empty;
        defer input_ordinals.deinit(alloc);
        for (self.owners, owners, 0..) |intent, *owner, i| {
            const binding = intent.binding;
            ranges[i] = intent.range;
            try binding.validate();
            if (intent.range.group_id != binding.fence.owner_group_id or intent.range.table_id != self.before.table_id or
                binding.fence.namespace.shard_id != tables.rangeDocIdentityShardId(intent.range) or
                binding.fence.namespace.range_id != tables.rangeDocIdentityRangeId(intent.range) or
                binding.before_version != before.runtime_schema.version or binding.after_version != after.runtime_schema.version or
                binding.columns != self.columns.len or !std.mem.eql(u8, &binding.before_schema, &before_schema_hash) or
                !std.mem.eql(u8, &binding.after_schema, &after_schema_hash) or
                !std.mem.eql(u8, &binding.before_public, &before_public_hash) or
                !std.mem.eql(u8, &binding.after_public, &after_public_hash) or
                !std.mem.eql(u8, &binding.projection_identity, &projection_digest))
                return error.InvalidGenerationPublication;
            const input = try input_ordinals.getOrPut(alloc, binding.inputs);
            if (!input.found_existing) input.value_ptr.* = @intCast(input_ordinals.count() - 1);
            owner.* = .{ .ordinal = @intCast(i), .input_ordinal = input.value_ptr.*, .binding = binding, .physical_root_incarnation = intent.physical_root_incarnation };
        }
        head.input_count = @intCast(input_ordinals.count());
        tables.validateCompleteKeyspaceRanges(ranges) catch return error.InvalidGenerationPublication;
        try barriers.validateCohort(head, owners);
        return head;
    }

    fn encodeAlloc(self: Plan, alloc: A) ![]u8 {
        // Count without allocating escaped schema strings, then reserve exactly
        // once. A malicious escape-heavy plan cannot grow an unbounded writer.
        var count = std.Io.Writer.Discarding.init(&.{});
        try std.json.Stringify.value(self, .{}, &count.writer);
        if (count.fullCount() > max_bytes) return error.InvalidGenerationPublication;
        const bytes = try alloc.alloc(u8, @intCast(count.fullCount()));
        errdefer alloc.free(bytes);
        var writer = std.Io.Writer.fixed(bytes);
        try std.json.Stringify.value(self, .{}, &writer);
        return bytes;
    }

    /// Recovery joins the retained immutable plan with the complete compact
    /// ledger. A copied terminal head or missing owner cannot certify success.
    /// Physical-root adoption is deliberately not inferred from this comparison.
    pub fn requireProgress(self: Plan, alloc: A, head: barriers.Head, owners: []const barriers.Owner) !void {
        const initial = try self.validate(alloc);
        if (initial.metadata_group_id != head.metadata_group_id or
            !std.mem.eql(u8, &initial.metadata_incarnation, &head.metadata_incarnation) or
            !std.mem.eql(u8, &initial.id, &head.id) or !std.mem.eql(u8, &initial.plan_digest, &head.plan_digest) or
            initial.table_id != head.table_id or initial.owner_count != head.owner_count or initial.input_count != head.input_count or
            !std.meta.eql(initial.reader_requirements, head.reader_requirements) or owners.len != self.owners.len)
            return error.InvalidGenerationPublication;
        var input_ordinals: std.AutoHashMapUnmanaged(Digest, u16) = .empty;
        defer input_ordinals.deinit(alloc);
        for (owners, self.owners) |current, intent| {
            const input = try input_ordinals.getOrPut(alloc, intent.binding.inputs);
            if (!input.found_existing) input.value_ptr.* = @intCast(input_ordinals.count() - 1);
            if (!std.meta.eql(current.binding, intent.binding) or current.initialRoot() != intent.physical_root_incarnation)
                return error.InvalidGenerationPublication;
            if (current.input_ordinal != input.value_ptr.*) return error.InvalidGenerationPublication;
        }
        try barriers.validateCohort(head, owners);
    }
};

pub const Prepared = struct {
    alloc: A,
    encoded: []u8,
    head: barriers.Head,
    owners: []barriers.Owner,

    /// Caller-supplied binding plan digests are replaced, not trusted. Every
    /// other binding component must match the independently derived successor.
    pub fn init(alloc: A, proposed: Plan) !Prepared {
        try proposed.requireBoundedShape();
        const intents = try alloc.dupe(OwnerIntent, proposed.owners);
        defer alloc.free(intents);
        var plan = proposed;
        plan.owners = intents;
        const commitment = try plan.digest();
        for (intents) |*intent| intent.binding.plan_digest = commitment;
        const head = try plan.validate(alloc);
        const encoded = try plan.encodeAlloc(alloc);
        errdefer alloc.free(encoded);
        const owners = try alloc.alloc(barriers.Owner, intents.len);
        errdefer alloc.free(owners);
        var input_ordinals: std.AutoHashMapUnmanaged(Digest, u16) = .empty;
        defer input_ordinals.deinit(alloc);
        for (intents, owners, 0..) |intent, *owner, i| {
            const input = try input_ordinals.getOrPut(alloc, intent.binding.inputs);
            if (!input.found_existing) input.value_ptr.* = @intCast(input_ordinals.count() - 1);
            owner.* = .{ .ordinal = @intCast(i), .input_ordinal = input.value_ptr.*, .binding = intent.binding, .physical_root_incarnation = intent.physical_root_incarnation };
        }
        return .{ .alloc = alloc, .encoded = encoded, .head = head, .owners = owners };
    }

    pub fn deinit(self: *Prepared) void {
        self.alloc.free(self.encoded);
        self.alloc.free(self.owners);
        self.* = undefined;
    }
};

pub fn parse(alloc: A, encoded: []const u8) !std.json.Parsed(Plan) {
    if (encoded.len == 0 or encoded.len > max_bytes) return error.InvalidGenerationPublication;
    var parsed = try std.json.parseFromSlice(Plan, alloc, encoded, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    _ = try parsed.value.validate(alloc);
    const canonical = try parsed.value.encodeAlloc(alloc);
    defer alloc.free(canonical);
    if (!std.mem.eql(u8, canonical, encoded)) return error.InvalidGenerationPublication;
    return parsed;
}

pub const test_support = if (@import("builtin").is_test) struct {
    pub const PlanFixture = Fixture;
} else struct {};
const Fixture = struct {
    alloc: A,
    after: records.TableRecord,
    intents: [2]OwnerIntent,
    const schema = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\",\"nullable\":true},\"n\":{\"type\":\"integer\",\"nullable\":true}},\"additionalProperties\":false}}}}";
    const before: records.TableRecord = .{ .table_id = 7, .name = "rows", .schema_json = schema, .indexes_json = "{\"named_text\":{\"type\":\"full_text\"}}" };
    pub fn init(a: A) !Fixture {
        const after = try api.prepareNullabilityPublicationRecord(a, &before, &.{"id"});
        errdefer tables.freeTable(a, after);
        var old = try local.schema_mod.PreparedSchemaDefinition.init(a, schema);
        defer old.deinit(a);
        var next = try local.schema_mod.PreparedSchemaDefinition.init(a, after.schema_json);
        defer next.deinit(a);
        const old_bytes = try local.storage_schema.serializeSchema(a, old.runtime_schema);
        defer a.free(old_bytes);
        const next_bytes = try local.storage_schema.serializeSchema(a, next.runtime_schema);
        defer a.free(next_bytes);
        var projection = contract.ProjectionHasher.init();
        try projection.add("id");
        var intents: [2]OwnerIntent = undefined;
        for (&intents, 0..) |*intent, i| intent.* = .{
            .range = .{ .group_id = 72 + i, .table_id = 7, .range_id = 272 + i, .doc_identity_shard_id = 172 + i, .doc_identity_range_id = 272 + i, .start_key = if (i == 0) "" else "m", .end_key = if (i == 0) "m" else null },
            .physical_root_incarnation = 17 + i,
            .binding = .{
                .id = @splat(1),
                .plan_digest = @splat(2),
                .owner = @splat(3),
                .fence = .{ .transition_id = 4, .attempt = 1, .admission_epoch = 1, .owner_group_id = 72 + i, .peer_group_id = 72 + i, .namespace = .{ .table_id = 7, .shard_id = 172 + i, .range_id = 272 + i }, .catalog_digest = @splat(5), .role = .schema_validation },
                .authority = if (i == 0) .native else .raft,
                .before_version = 1,
                .after_version = 2,
                .columns = 1,
                .before_schema = contract.hash(old_bytes),
                .after_schema = contract.hash(next_bytes),
                .before_public = contract.hash(schema),
                .after_public = contract.hash(after.schema_json),
                .after_catalog = @splat(10),
                .physical_catalog = @splat(11),
                .inputs = @splat(12),
                .projection_identity = try projection.finish(),
            },
        };
        return .{ .alloc = a, .after = after, .intents = intents };
    }
    pub fn plan(self: *const Fixture) Plan {
        return .{ .metadata_group_id = 21, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .id = @splat(1), .before = before, .after = self.after, .columns = &.{"id"}, .owners = &self.intents };
    }
    pub fn deinit(self: Fixture) void {
        tables.freeTable(self.alloc, self.after);
    }
};

test "schema validation publication immutable plan binds exact successor cohort and recovery ledger" {
    const a = std.testing.allocator;
    // Contract fixture only: physical roots/catalog cuts still require real
    // authenticated owner preparation in the accepting metadata transaction.
    var fixture = try Fixture.init(a);
    defer fixture.deinit();
    var prepared = try Prepared.init(a, fixture.plan());
    defer prepared.deinit();
    var decoded = try parse(a, prepared.encoded);
    defer decoded.deinit();
    try decoded.value.requireProgress(a, prepared.head, prepared.owners);
    try std.testing.expectEqualStrings("named_text", decoded.value.after.full_text_read_index.?);
    try std.testing.expectEqualStrings(Fixture.before.indexes_json, decoded.value.after.indexes_json);
    var repeated = try Prepared.init(a, decoded.value);
    defer repeated.deinit();
    try std.testing.expectEqualSlices(u8, prepared.encoded, repeated.encoded);
    try std.testing.expectEqualDeep(prepared.head, repeated.head);
    const spaced = try std.mem.concat(a, u8, &.{ prepared.encoded, " " });
    defer a.free(spaced);
    try std.testing.expectError(error.InvalidGenerationPublication, parse(a, spaced));
    try std.testing.expectError(error.InvalidGenerationPublication, decoded.value.requireProgress(a, prepared.head, prepared.owners[0..1]));
    const saved = prepared.owners[0].physical_root_incarnation;
    prepared.owners[0].physical_root_incarnation += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, decoded.value.requireProgress(a, prepared.head, prepared.owners));
    prepared.owners[0].physical_root_incarnation = saved;
    var changed = decoded.value;
    changed.metadata_group_id += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, changed.validate(a));
    changed = decoded.value;
    changed.after.indexes_json = "{}";
    try std.testing.expectError(error.InvalidGenerationPublication, changed.validate(a));
    changed = decoded.value;
    changed.columns = &.{"n"};
    try std.testing.expectError(error.InvalidGenerationPublication, changed.validate(a));
    const duplicate = [_]OwnerIntent{ decoded.value.owners[0], decoded.value.owners[0] };
    changed = decoded.value;
    changed.owners = &duplicate;
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, changed));
    const foreign_root = [_]OwnerIntent{ decoded.value.owners[0], decoded.value.owners[1] };
    var changed_root = foreign_root;
    changed_root[1].physical_root_incarnation += 1;
    changed = decoded.value;
    changed.owners = &changed_root;
    try std.testing.expectError(error.InvalidGenerationPublication, changed.validate(a));
    var gap = foreign_root;
    gap[1].range.start_key = "n";
    changed = decoded.value;
    changed.owners = &gap;
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, changed));
    var guard_plan = fixture.plan();
    guard_plan.table_guard = .{ .target = .{ .name = "rows" }, .logical_table = "rows", .owner = .{ .table_id = 7, .schema_version = 1, .schema_digest = contract.hash(Fixture.schema), .kind = .table }, .incarnation = @splat(3) };
    var guarded = try Prepared.init(a, guard_plan);
    defer guarded.deinit();
    try std.testing.expect(!std.mem.eql(u8, &guarded.head.plan_digest, &prepared.head.plan_digest));
    guard_plan.table_guard.?.owner.schema_version += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, guard_plan));
}

test "schema validation publication immutable inputs use dense digest deduplication across owners" {
    const a = std.testing.allocator;
    var fixture = try Fixture.init(a);
    defer fixture.deinit();
    var shared = try Prepared.init(a, fixture.plan());
    defer shared.deinit();
    try std.testing.expectEqual(@as(u16, 1), shared.head.input_count);
    try std.testing.expectEqual(@as(u16, 0), shared.owners[1].input_ordinal);
    fixture.intents[1].binding.inputs[0] ^= 1;
    var distinct = try Prepared.init(a, fixture.plan());
    defer distinct.deinit();
    try std.testing.expectEqual(@as(u16, 2), distinct.head.input_count);
    try std.testing.expectEqual(@as(u16, 0), distinct.owners[0].input_ordinal);
    try std.testing.expectEqual(@as(u16, 1), distinct.owners[1].input_ordinal);
    var plan = try parse(a, distinct.encoded);
    defer plan.deinit();
    try plan.value.requireProgress(a, distinct.head, distinct.owners);
    distinct.owners[1].input_ordinal = 0;
    try std.testing.expectError(error.InvalidGenerationPublication, plan.value.requireProgress(a, distinct.head, distinct.owners));
}

test "schema validation publication immutable plan preparation and canonical decoding are allocation atomic" {
    const a = std.testing.allocator;
    var fixture = try Fixture.init(a);
    defer fixture.deinit();
    const Faults = struct {
        fn prepare(backing: A, proposed: Plan) !void {
            var result = try Prepared.init(backing, proposed);
            defer result.deinit();
        }
        fn decode(backing: A, encoded: []const u8) !void {
            var result = try parse(backing, encoded);
            defer result.deinit();
        }
    };
    // Arena remap success depends on neighboring allocations. Force the
    // allocate/copy fallback so every allocation cut is deterministic.
    var no_resize = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Faults.prepare, .{fixture.plan()});
    var prepared = try Prepared.init(a, fixture.plan());
    defer prepared.deinit();
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Faults.decode, .{prepared.encoded});
    var oversized = fixture.plan();
    oversized.columns = &.{""};
    try std.testing.expectError(error.InvalidNullabilityProjection, Prepared.init(a, oversized));
    oversized = fixture.plan();
    oversized.before.schema_json = @as([*]const u8, @ptrFromInt(1))[0 .. contract.max_schema + 1];
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, oversized));
}

test "schema validation publication immutable plan prepares the maximum owner cohort with shared schema work" {
    const a = std.testing.allocator;
    var fixture = try Fixture.init(a);
    defer fixture.deinit();
    const intents = try a.alloc(OwnerIntent, barriers.max_owners);
    defer a.free(intents);
    const boundaries = try a.alloc([16]u8, barriers.max_owners);
    defer a.free(boundaries);
    for (boundaries, 0..) |*boundary, i| _ = try std.fmt.bufPrint(boundary, "{x:0>16}", .{i});
    for (intents, 0..) |*intent, i| {
        intent.* = fixture.intents[i % fixture.intents.len];
        intent.physical_root_incarnation = 17 + i;
        intent.binding.fence.owner_group_id = 72 + i;
        intent.binding.fence.peer_group_id = 72 + i;
        intent.binding.fence.namespace.shard_id = 172 + i;
        intent.binding.fence.namespace.range_id = 272 + i;
        intent.range = .{ .group_id = 72 + i, .table_id = 7, .range_id = 272 + i, .doc_identity_shard_id = 172 + i, .doc_identity_range_id = 272 + i, .start_key = if (i == 0) "" else &boundaries[i], .end_key = if (i + 1 == intents.len) null else &boundaries[i + 1] };
    }
    var proposed = fixture.plan();
    proposed.owners = intents;
    var prepared = try Prepared.init(a, proposed);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(u16, barriers.max_owners), prepared.head.owner_count);
    try std.testing.expectEqual(@as(usize, barriers.max_owners), prepared.owners.len);
    try std.testing.expect(prepared.encoded.len < max_bytes);
    try barriers.validateCohort(prepared.head, prepared.owners);
}
