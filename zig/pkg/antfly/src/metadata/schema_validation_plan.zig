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

pub const max_participants = 128;
pub const TablePair = struct { before: records.TableRecord, after: records.TableRecord };
pub const TypeChange = struct {
    request: @import("../system_catalog/named_types.zig").Mutation,
    before: local.sql_catalog.named_types.Record,
    after: local.sql_catalog.named_types.Record,

    fn validate(self: @This(), a: A) !void {
        try self.request.validate();
        if (self.request.operation != .add_enum and self.request.operation != .set_domain_not_null) return error.InvalidGenerationPublication;
        try self.before.validate(a);
        try self.after.validate(a);
        if ((self.request.operation == .set_domain_not_null) != (self.before.definition == .domain)) return error.InvalidGenerationPublication;
        try self.before.requireEpochSuccessor(self.after);
        const captured = switch (self.request.operation) {
            .add_enum => |change| change.expected,
            .set_domain_not_null => |change| change.expected,
            else => unreachable,
        };
        if (self.request.namespace_id != self.before.identity.namespace_id or captured.id != self.before.identity.id or
            captured.generation != self.before.identity.generation) return error.InvalidGenerationPublication;
        var definition: ?local.sql_catalog.named_types.Owned = null;
        defer if (definition) |*owned| owned.deinit();
        var expected = self.before;
        expected.identity.generation += 1;
        switch (self.request.operation) {
            .add_enum => |add| {
                definition = try local.sql_catalog.named_types.Owned.add(a, self.before.definition, add.label, add.placement);
                expected.definition = definition.?.definition;
            },
            .set_domain_not_null => expected.definition.domain.not_null = true,
            else => unreachable,
        }
        const actual_bytes = try self.after.encode(a);
        defer a.free(actual_bytes);
        const expected_bytes = try expected.encode(a);
        defer a.free(expected_bytes);
        if (!std.mem.eql(u8, actual_bytes, expected_bytes)) return error.InvalidGenerationPublication;
    }
};

pub const SequenceChange = struct {
    request: @import("../system_catalog/sequence_catalog.zig").Mutation,
    before: local.system_catalog_sequences.Descriptor,
    database_id: u64,
    references: @import("../system_catalog/sequence_default_dependencies.zig").Stamp,

    fn validate(self: @This()) !void {
        try self.request.validate();
        _ = try self.before.definition();
        if (self.before.internal_identity) return error.SqlDependentObjectsExist;
        if (self.request.operation != .drop or !self.request.operation.drop.cascade or
            self.database_id == 0 or self.references.revision == 0 or self.references.count == 0 or self.references.count > max_participants * contract.max_columns) return error.InvalidGenerationPublication;
        const expected = self.request.operation.drop.expected orelse return error.InvalidGenerationPublication;
        if (expected.id != self.before.id or expected.generation != self.before.generation) return error.InvalidGenerationPublication;
    }
    pub fn selector(self: @This()) local.system_catalog_column_sequences.DefaultRetirement {
        return .{ .sequence_id = self.before.id, .namespace_id = self.request.namespace_id, .incarnation = self.request.incarnation };
    }
};

/// Compact immutable membership for bounded terminal cleanup and targeted
/// participant discovery. A digest is a commitment, never mutation authority.
pub const Participants = struct {
    len: u16,
    tables: [max_participants]u64 = @splat(0),
    pub fn validate(self: @This()) !void {
        if (self.len == 0 or self.len > max_participants) return error.InvalidGenerationPublication;
        var previous: u64 = 0;
        for (self.tables, 0..) |id, i| {
            if (i >= self.len) {
                if (id != 0) return error.InvalidGenerationPublication;
            } else {
                if (id <= previous) return error.InvalidGenerationPublication;
                previous = id;
            }
        }
    }
    pub fn contains(self: @This(), id: u64) bool {
        var low: usize = 0;
        var high: usize = self.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            if (self.tables[middle] == id) return true;
            if (self.tables[middle] < id) low = middle + 1 else high = middle;
        }
        return false;
    }
    pub fn digest(self: @This()) !Digest {
        try self.validate();
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/schema-participants/v1\x00");
        for (self.tables[0..self.len]) |id| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, id, .little);
            state.update(&bytes);
        }
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }
};
pub const ParticipantsRecord = barriers.Record(Participants, "SVPT", 4096);

pub const Plan = struct {
    version: u16 = 2,
    metadata_group_id: u64,
    metadata_incarnation: local.metadata_incarnation.MetadataClusterIncarnation,
    id: [16]u8,
    table_guard: ?local.system_catalog_domain.RelationMutationGuard = null,
    before: records.TableRecord,
    after: records.TableRecord,
    selection: contract.InputSelection,
    owners: []const OwnerIntent,
    additional_tables: []const TablePair = &.{},
    type_change: ?TypeChange = null,
    sequence_change: ?SequenceChange = null,
    retirement_root: ?@import("table_retirement_authority.zig").Authority = null,

    pub fn cohort(self: Plan) bool {
        return self.type_change != null or self.sequence_change != null or self.retirement_root != null;
    }

    pub fn participantCount(self: Plan) usize {
        return 1 + self.additional_tables.len;
    }
    pub fn participant(self: Plan, index: usize) TablePair {
        return if (index == 0) .{ .before = self.before, .after = self.after } else self.additional_tables[index - 1];
    }
    pub fn participantDirectory(self: Plan) !Participants {
        if (self.participantCount() > max_participants) return error.InvalidGenerationPublication;
        var result: Participants = .{ .len = @intCast(self.participantCount()) };
        for (0..self.participantCount()) |index| result.tables[index] = self.participant(index).before.table_id;
        try result.validate();
        return result;
    }
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            const skip = (comptime std.mem.eql(u8, field, "additional_tables")) and self.additional_tables.len == 0 or
                (comptime std.mem.eql(u8, field, "type_change")) and self.type_change == null or
                (comptime std.mem.eql(u8, field, "sequence_change")) and self.sequence_change == null or
                (comptime std.mem.eql(u8, field, "retirement_root")) and self.retirement_root == null;
            if (!skip) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }

    fn requireBoundedShape(self: Plan) !void {
        // Subtractive dependency changes belong to the complete owning-table
        // retirement cohort, never an independent validation publication.
        if ((self.selection == .dependent_object_retirement) != (self.retirement_root != null)) return error.InvalidGenerationPublication;
        if (self.retirement_root) |root| {
            try root.validate();
            if (self.version != 5 or self.type_change != null or self.sequence_change != null or self.table_guard != null or
                self.metadata_group_id != root.root.group or self.participantCount() != root.effects.survivors or
                std.mem.eql(u8, &self.id, &root.root.id) or self.selection.dependent_object_retirement.relations.len != 1 or
                self.selection.dependent_object_retirement.relations[0].table_id != root.root.table) return error.InvalidGenerationPublication;
        }
        if ((self.type_change != null and self.sequence_change != null) or
            (!self.cohort() and (self.version != 2 or self.additional_tables.len != 0 or self.selection == .enum_epoch or self.selection == .domain_not_null or self.selection == .sequence_default_removal)) or
            (self.type_change != null and (self.version != 3 or (self.selection != .enum_epoch and self.selection != .domain_not_null) or self.table_guard != null)) or
            (self.sequence_change != null and (self.version != 4 or self.selection != .sequence_default_removal or self.table_guard != null)) or
            self.participantCount() > max_participants or self.metadata_group_id == 0 or
            !local.metadata_incarnation.isValid(self.metadata_incarnation) or std.mem.allEqual(u8, &self.id, 0) or
            self.owners.len == 0 or self.owners.len > barriers.max_owners or
            self.selection.count() == 0 or self.selection.count() > contract.max_columns or
            self.before.schema_json.len > contract.max_schema or self.after.schema_json.len > contract.max_schema or
            self.before.indexes_json.len > contract.max_schema or self.after.indexes_json.len > contract.max_schema or
            self.before.storage_migration != null or self.after.storage_migration != null)
            return error.InvalidGenerationPublication;
        var raw_bytes: usize = 0;
        for (0..self.participantCount()) |index| {
            const pair = self.participant(index);
            for ([_]records.TableRecord{ pair.before, pair.after }) |table| {
                if (table.schema_json.len > contract.max_schema or table.indexes_json.len > contract.max_schema or
                    table.read_schema_json.len > contract.max_schema or table.storage_migration != null) return error.InvalidGenerationPublication;
                inline for (@typeInfo(records.TableRecord).@"struct".field_names) |field| {
                    if (comptime @TypeOf(@field(table, field)) == []const u8) {
                        raw_bytes = std.math.add(usize, raw_bytes, @field(table, field).len) catch return error.InvalidGenerationPublication;
                        if (raw_bytes > max_bytes) return error.InvalidGenerationPublication;
                    }
                }
                if (table.full_text_read_index) |name| if (name.len == 0 or name.len > 1024) return error.InvalidGenerationPublication;
            }
            try self.selection.validate();
        }
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
            if (guard.hasDefinitionIntent() or guard.owner.kind != .table or guard.owner.table_id != self.before.table_id or
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
        hash.update(if (self.retirement_root != null) "antfly/schema-validation-immutable-plan/v5" else if (self.sequence_change != null) "antfly/schema-validation-immutable-plan/v4" else if (self.type_change != null) "antfly/schema-validation-immutable-plan/v3" else "antfly/schema-validation-immutable-plan/v2");
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(hash, &.{});
        try std.json.Stringify.value(.{
            .version = self.version,
            .metadata_group_id = self.metadata_group_id,
            .metadata_incarnation = self.metadata_incarnation,
            .id = self.id,
            .table_guard = self.table_guard,
            .before = self.before,
            .after = self.after,
            .selection = self.selection,
            .owner_count = self.owners.len,
        }, .{}, &writer.writer);
        if (self.type_change != null) {
            try writer.writer.writeByte(0);
            try std.json.Stringify.value(.{ .additional_tables = self.additional_tables, .type_change = self.type_change }, .{}, &writer.writer);
        }
        if (self.sequence_change != null) {
            try writer.writer.writeByte(0);
            try std.json.Stringify.value(.{ .additional_tables = self.additional_tables, .sequence_change = self.sequence_change }, .{}, &writer.writer);
        }
        if (self.retirement_root) |root| {
            try writer.writer.writeByte(0);
            try std.json.Stringify.value(.{ .additional_tables = self.additional_tables, .retirement_root = root }, .{}, &writer.writer);
        }
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
        if (self.cohort()) return self.validateCatalogCohort(alloc);
        const logical = try self.selection.logical(alloc);
        defer alloc.free(logical.names());
        const projection_digest: ?Digest = if (self.selection == .not_null) block: {
            var projection = contract.ProjectionHasher.init();
            for (self.selection.not_null) |column| try projection.add(column);
            break :block try projection.finish();
        } else if (self.selection == .object_rename) try local.system_catalog_domain.object_definitions.renameFingerprint(self.selection.object_rename) else null;
        if (self.selection == .foreign_keys) {
            if (!std.mem.eql(u8, self.selection.foreign_keys.table_name, self.before.name)) return error.InvalidGenerationPublication;
            for (self.selection.foreign_keys.parents) |parent| if (parent.parent_definition == null) return error.InvalidGenerationPublication;
        }
        const replacement = try api.prepareSchemaValidationPublicationRecord(alloc, &self.before, logical);
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
            if (binding.purpose != self.selection.purpose()) return error.InvalidGenerationPublication;
            if (intent.range.group_id != binding.fence.owner_group_id or intent.range.table_id != self.before.table_id or
                binding.fence.namespace.shard_id != tables.rangeDocIdentityShardId(intent.range) or
                binding.fence.namespace.range_id != tables.rangeDocIdentityRangeId(intent.range) or
                binding.before_version != before.runtime_schema.version or binding.after_version != after.runtime_schema.version or
                binding.columns != self.selection.count() or !std.mem.eql(u8, &binding.before_schema, &before_schema_hash) or
                !std.mem.eql(u8, &binding.after_schema, &after_schema_hash) or
                !std.mem.eql(u8, &binding.before_public, &before_public_hash) or
                !std.mem.eql(u8, &binding.after_public, &after_public_hash) or
                (if (projection_digest) |expected| !std.mem.eql(u8, &binding.projection_identity, &expected) else false))
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

    /// Pure, bounded preparation. Native hashes are attested by each owner,
    /// not inferred from untrusted claims. Production admission MUST rejoin
    /// every signed preparation and the current metadata/type/owner cut.
    fn validateCatalogCohort(self: Plan, a: A) !barriers.Head {
        const directory = try self.participantDirectory();
        const commitment = try self.digest();
        var head: barriers.Head = .{ .metadata_group_id = self.metadata_group_id, .metadata_incarnation = self.metadata_incarnation, .id = self.id, .plan_digest = commitment, .table_id = self.before.table_id, .owner_count = @intCast(self.owners.len), .participant_count = directory.len, .participants_digest = try directory.digest() };
        var projection: Digest = undefined;
        var incarnation: [16]u8 = undefined;
        if (self.type_change) |change| {
            try change.validate(a);
            const is_domain = change.request.operation == .set_domain_not_null;
            if (is_domain != (self.selection == .domain_not_null)) return error.InvalidGenerationPublication;
            const epoch = if (is_domain) self.selection.domain_not_null else self.selection.enum_epoch;
            if (epoch.type_id != change.before.identity.id or epoch.generation != change.before.identity.generation or
                !std.mem.eql(u8, &epoch.origin_id, &change.before.identity.origin_id)) return error.InvalidGenerationPublication;
            incarnation = change.request.incarnation;
            projection = try epoch.fingerprint();
            head.type_id = change.before.identity.id;
            head.type_generation = change.after.identity.generation;
            head.type_admission_digest = try @import("../system_catalog/named_types.zig").boundDigest(a, try @import("../system_catalog/named_types.zig").observation(a, change.request));
        } else if (self.retirement_root) |root| {
            head.retirement_root = root;
            projection = try self.selection.dependent_object_retirement.fingerprint();
            _ = std.fmt.hexToBytes(&incarnation, &self.metadata_incarnation) catch return error.MetadataIncarnationMismatch;
        } else {
            const change = self.sequence_change orelse return error.InvalidGenerationPublication;
            try change.validate();
            if (self.selection != .sequence_default_removal or !std.meta.eql(self.selection.sequence_default_removal, change.selector())) return error.InvalidGenerationPublication;
            projection = try change.selector().fingerprint();
            incarnation = change.request.incarnation;
            head.sequence_retirement = .{ .id = change.before.id, .generation = change.before.generation, .namespace_id = change.request.namespace_id, .database_id = change.database_id, .admission_digest = try @import("../system_catalog/sequence_catalog.zig").commandDigest(a, change.request) };
        }
        if (!std.mem.eql(u8, &std.fmt.bytesToHex(incarnation, .lower), &self.metadata_incarnation)) return error.MetadataIncarnationMismatch;
        var floor: local.common_schema_readers.ReaderFloor = .{};
        const owners = try a.alloc(barriers.Owner, self.owners.len);
        defer a.free(owners);
        const ranges = try a.alloc(records.RangeRecord, self.owners.len);
        defer a.free(ranges);
        var inputs: std.AutoHashMapUnmanaged(Digest, u16) = .empty;
        defer inputs.deinit(a);
        var ordinal: usize = 0;
        for (0..self.participantCount()) |index| {
            const pair = self.participant(index);
            if (pair.before.table_id != pair.after.table_id or pair.before.storage.engine != .local or pair.before.storage_migration != null or
                pair.after.storage_migration != null or pair.before.read_schema_json.len != 0 or pair.before.restore_backup_id.len != 0 or
                pair.before.relational_retirement_json.len != 0) return error.InvalidGenerationPublication;
            var expected = try tables.cloneTable(a, pair.before);
            defer tables.freeTable(a, expected);
            const schema = if (self.type_change) |change|
                try local.sql_catalog.named_layouts.epochSchemaAlloc(a, pair.before.schema_json, change.before, change.after)
            else if (self.retirement_root != null)
                try self.selection.dependent_object_retirement.schemaAlloc(a, pair.before.table_id, pair.before.schema_json)
            else
                try self.sequence_change.?.selector().schemaAlloc(a, pair.before.table_id, pair.before.schema_json);
            a.free(expected.schema_json);
            expected.schema_json = schema;
            if (!tables.tableDefinitionsEqual(expected, pair.after)) return error.InvalidGenerationPublication;
            if (pair.before.schema_reader_requirements) |required| floor = try floor.include(required);
            if (pair.after.schema_reader_requirements) |required| floor = try floor.include(required);
            const before_version = try api.schemaVersion(pair.before.schema_json);
            const after_version = try api.schemaVersion(pair.after.schema_json);
            const before_public = contract.hash(pair.before.schema_json);
            const after_public = contract.hash(pair.after.schema_json);
            var count: usize = 0;
            while (ordinal < self.owners.len and self.owners[ordinal].range.table_id == pair.before.table_id) : (ordinal += 1) {
                const intent = self.owners[ordinal];
                const binding = intent.binding;
                try binding.validate();
                if (binding.purpose != self.selection.purpose() or binding.fence.namespace.table_id != pair.before.table_id or binding.columns != 1 or
                    intent.range.group_id != binding.fence.owner_group_id or binding.fence.namespace.shard_id != tables.rangeDocIdentityShardId(intent.range) or
                    binding.fence.namespace.range_id != tables.rangeDocIdentityRangeId(intent.range) or binding.before_version != before_version or
                    binding.after_version != after_version or !std.mem.eql(u8, &binding.before_public, &before_public) or
                    !std.mem.eql(u8, &binding.after_public, &after_public) or !std.mem.eql(u8, &binding.projection_identity, &projection)) return error.InvalidGenerationPublication;
                const input = try inputs.getOrPut(a, binding.inputs);
                if (!input.found_existing) input.value_ptr.* = @intCast(inputs.count() - 1);
                owners[ordinal] = .{ .ordinal = @intCast(ordinal), .input_ordinal = input.value_ptr.*, .binding = binding, .physical_root_incarnation = intent.physical_root_incarnation };
                ranges[count] = intent.range;
                count += 1;
            }
            if (count == 0) return error.InvalidGenerationPublication;
            tables.validateCompleteKeyspaceRanges(ranges[0..count]) catch return error.InvalidGenerationPublication;
        }
        if (ordinal != self.owners.len) return error.InvalidGenerationPublication;
        head.input_count = @intCast(inputs.count());
        head.reader_requirements = if (floor.active()) .{ .semantic_version = floor.semantic_version, .encoding_version = floor.encoding_version } else null;
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
            !std.meta.eql(initial.retirement_root, head.retirement_root) or !std.meta.eql(initial.sequence_retirement, head.sequence_retirement) or initial.type_id != head.type_id or initial.type_generation != head.type_generation or !std.mem.eql(u8, &initial.type_admission_digest, &head.type_admission_digest) or initial.participant_count != head.participant_count or !std.mem.eql(u8, &initial.participants_digest, &head.participants_digest) or
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
    participants: ?Participants = null,
    type_request: ?std.json.Parsed(@import("../system_catalog/named_types.zig").Mutation) = null,
    type_after: ?@import("../system_catalog/named_types.zig").Identity = null,
    sequence_request: ?std.json.Parsed(@import("../system_catalog/sequence_catalog.zig").Mutation) = null,

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
        var type_request: ?std.json.Parsed(@import("../system_catalog/named_types.zig").Mutation) = null;
        errdefer if (type_request) |*value| value.deinit();
        if (plan.type_change) |change| {
            const bytes = try std.json.Stringify.valueAlloc(alloc, change.request, .{});
            defer alloc.free(bytes);
            type_request = try std.json.parseFromSlice(@import("../system_catalog/named_types.zig").Mutation, alloc, bytes, .{ .allocate = .alloc_always });
        }
        var sequence_request: ?std.json.Parsed(@import("../system_catalog/sequence_catalog.zig").Mutation) = null;
        errdefer if (sequence_request) |*value| value.deinit();
        if (plan.sequence_change) |change| {
            const bytes = try std.json.Stringify.valueAlloc(alloc, change.request, .{});
            defer alloc.free(bytes);
            sequence_request = try std.json.parseFromSlice(@import("../system_catalog/sequence_catalog.zig").Mutation, alloc, bytes, .{ .allocate = .alloc_always });
        }
        return .{ .alloc = alloc, .encoded = encoded, .head = head, .owners = owners, .sequence_request = sequence_request, .participants = if (plan.cohort()) try plan.participantDirectory() else null, .type_request = type_request, .type_after = if (plan.type_change) |change| .{ .id = change.after.identity.id, .generation = change.after.identity.generation } else null };
    }

    pub fn deinit(self: *Prepared) void {
        if (self.type_request) |*value| value.deinit();
        if (self.sequence_request) |*value| value.deinit();
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
    pub const TypeCohort = TypeCohortFixture;
    pub const SequenceCohort = SequenceCohortFixture;
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
        return .{ .metadata_group_id = 21, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .id = @splat(1), .before = before, .after = self.after, .selection = .{ .not_null = &.{"id"} }, .owners = &self.intents };
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
    changed.selection = .{ .not_null = &.{"n"} };
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
    oversized.selection = .{ .not_null = &.{""} };
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

test "schema validation publication rejects FK role substitution under a NOT NULL selection" {
    const a = std.testing.allocator;
    var fixture = try Fixture.init(a);
    defer fixture.deinit();
    for ([_]contract.Purpose{ .foreign_key_source, .foreign_key_dual }) |purpose| {
        fixture.intents[0].binding.purpose = purpose;
        // Prepared.init recomputes the plan commitment. Refusal must be the
        // role/cohort guard rather than an accidentally stale plan digest.
        try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, fixture.plan()));
    }
}

const TypeCohortFixture = struct {
    arena: std.heap.ArenaAllocator,
    value: Plan,
    inputs: []const []const u8,
    pub fn init(backing: A) !@This() {
        return initKind(backing, false);
    }
    pub fn initKind(backing: A, is_domain: bool) !@This() {
        var arena = std.heap.ArenaAllocator.init(backing);
        errdefer arena.deinit();
        const a = arena.allocator();
        const types = local.sql_catalog.named_types;
        const layouts = local.sql_catalog.named_layouts;
        const labels = try types.Owned.create(a, &.{ "z", "a" });
        const previous: types.Record = .{ .identity = try types.Identity.init(17, 2, 1, @splat(8)), .definition = if (is_domain) .{ .domain = .{ .base = .{ .builtin = .text } } } else labels.definition, .owner = .auth_disabled };
        const next_labels = try types.Owned.add(a, labels.definition, "middle", .{ .before = "a" });
        var successor = previous;
        successor.identity.generation += 1;
        if (is_domain) successor.definition.domain.not_null = true else successor.definition = next_labels.definition;
        const temporary = a;
        var root = try std.json.parseFromSliceLeaky(std.json.Value, temporary,
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"state":{"type":"sql_enum","x-antfly-sql-type-id":17,"nullable":true}},"additionalProperties":false}}}}
        , .{});
        if (is_domain) try root.object.getPtr("document_schemas").?.object.getPtr("row").?.object.getPtr("schema").?.object.getPtr("properties").?.object.getPtr("state").?.object.put(a, "type", .{ .string = "keyword" });
        try layouts.canonicalizeSchemaLeaky(temporary, &root, &.{previous});
        const before_json = try std.json.Stringify.valueAlloc(a, root, .{});
        const after_json = try layouts.epochSchemaAlloc(a, before_json, previous, successor);
        const first: records.TableRecord = .{ .table_id = 7, .name = "first", .schema_json = before_json, .indexes_json = "{}", .schema_reader_requirements = .{ .semantic_version = 40, .encoding_version = 40 } };
        var second = first;
        second.table_id = 8;
        second.name = "second";
        var second_root = try std.json.parseFromSliceLeaky(std.json.Value, a, before_json, .{ .allocate = .alloc_always });
        try second_root.object.put(a, "version", .{ .integer = 0 });
        const note = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"type\":\"keyword\",\"nullable\":true}", .{});
        try second_root.object.getPtr("document_schemas").?.object.getPtr("row").?.object.getPtr("schema").?.object.getPtr("properties").?.object.put(a, "note", note);
        second.schema_json = try std.json.Stringify.valueAlloc(a, second_root, .{});
        var first_after = first;
        first_after.schema_json = after_json;
        var second_after = second;
        second_after.schema_json = try layouts.epochSchemaAlloc(a, second.schema_json, previous, successor);
        const epoch: contract.EnumEpoch = .{ .type_id = 17, .generation = 1, .origin_id = @splat(8) };
        const owners = try a.alloc(OwnerIntent, 2);
        for (owners, 0..) |*owner, i| {
            const table_id = 7 + @as(u64, @intCast(i));
            const group = 72 + @as(u64, @intCast(i));
            owner.* = .{ .range = .{ .table_id = table_id, .group_id = group, .range_id = 272 + i, .doc_identity_shard_id = 172 + i, .doc_identity_range_id = 272 + i, .start_key = "", .end_key = null }, .physical_root_incarnation = 17 + i, .binding = .{ .purpose = if (is_domain) .domain_not_null else .enum_epoch, .id = @splat(1), .plan_digest = @splat(2), .owner = @splat(3), .fence = .{ .transition_id = 1, .attempt = 1, .admission_epoch = 1, .owner_group_id = group, .peer_group_id = group, .namespace = .{ .table_id = table_id, .shard_id = 172 + i, .range_id = 272 + i }, .catalog_digest = @splat(5), .role = .schema_validation }, .authority = .native, .before_version = 1, .after_version = 2, .columns = 1, .before_schema = @splat(6), .after_schema = @splat(7), .before_public = contract.hash(before_json), .after_public = contract.hash(after_json), .after_catalog = @splat(9), .physical_catalog = @splat(10), .inputs = @splat(@as(u8, @intCast(20 + i))), .projection_identity = try epoch.fingerprint() } };
        }
        const extra = try a.alloc(TablePair, 1);
        extra[0] = .{ .before = second, .after = second_after };
        const plan: Plan = .{ .version = 3, .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .id = @splat(1), .before = first, .after = first_after, .selection = if (is_domain) .{ .domain_not_null = epoch } else .{ .enum_epoch = epoch }, .owners = owners, .additional_tables = extra, .type_change = .{ .before = previous, .after = successor, .request = .{ .admission = .{ .head = 0, .nonce = @splat(11) }, .target = .{ .table = "state_kind" }, .namespace_id = 2, .incarnation = @splat(0x11), .operation = if (is_domain) .{ .set_domain_not_null = .{ .expected = .{ .id = 17, .generation = 1 } } } else .{ .add_enum = .{ .expected = .{ .id = 17, .generation = 1 }, .label = "middle", .placement = .{ .before = "a" } } } } } };

        const selector = try std.json.Stringify.valueAlloc(a, epoch, .{});
        var inputs: std.ArrayList([]const u8) = .empty;
        for (owners, 0..) |*owner, index| {
            const pair = plan.participant(index);
            var before = try local.schema_mod.PreparedSchemaDefinition.init(a, pair.before.schema_json);
            defer before.deinit(a);
            var after = try local.schema_mod.PreparedSchemaDefinition.init(a, pair.after.schema_json);
            defer after.deinit(a);
            const native_before = try local.storage_schema.serializeSchema(a, before.runtime_schema);
            const native_after = try local.storage_schema.serializeSchema(a, after.runtime_schema);
            const table_id = owner.range.table_id;
            var catalog = try local.schema_relational_declarations.compileCatalog(a, before.public_schema, table_id, null);
            defer catalog.deinit();
            var successor_catalog = try local.schema_relational_declarations.compileCatalog(a, after.public_schema, table_id, catalog.value);
            defer successor_catalog.deinit();
            const raw = try contract.encodeInputs(a, .{ pair.before.schema_json, pair.after.schema_json, catalog.value, selector });
            const known = for (inputs.items) |input| {
                if (std.mem.eql(u8, input, raw)) break true;
            } else false;
            if (!known) try inputs.append(a, raw);
            owner.binding.before_version = before.runtime_schema.version;
            owner.binding.after_version = after.runtime_schema.version;
            owner.binding.before_public = contract.hash(pair.before.schema_json);
            owner.binding.after_public = contract.hash(pair.after.schema_json);
            owner.binding.before_schema = contract.hash(native_before);
            owner.binding.after_schema = contract.hash(native_after);
            owner.binding.fence.catalog_digest = contract.hash(catalog.value);
            owner.binding.after_catalog = contract.hash(successor_catalog.value);
            owner.binding.inputs = contract.hash(raw);
        }
        return .{ .arena = arena, .value = plan, .inputs = try inputs.toOwnedSlice(a) };
    }
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

test "schema validation publication type cohorts bind complete table membership exact additive postimages and compact cleanup identities" {
    const a = std.testing.allocator;
    var fixture = try TypeCohortFixture.init(a);
    defer fixture.deinit();
    const plan = fixture.value;
    var prepared = try Prepared.init(a, plan);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(u16, 2), prepared.head.participant_count);
    try std.testing.expect(prepared.owners[0].binding.before_version != prepared.owners[1].binding.before_version);
    try std.testing.expect(!std.mem.eql(u8, &prepared.owners[0].binding.before_schema, &prepared.owners[1].binding.before_schema));
    try std.testing.expect(!std.mem.eql(u8, &prepared.owners[0].binding.before_public, &prepared.owners[1].binding.before_public));
    {
        var foreign = try a.dupe(barriers.Owner, prepared.owners);
        defer a.free(foreign);
        foreign[1].binding.projection_identity[0] ^= 1;
        try std.testing.expectError(error.InvalidGenerationPublication, barriers.validateCohort(prepared.head, foreign));
        foreign[1] = prepared.owners[1];
        foreign[1].binding.fence.namespace.table_id = foreign[0].binding.fence.namespace.table_id;
        var same_table_head = prepared.head;
        same_table_head.participant_count = 1;
        try std.testing.expectError(error.InvalidGenerationPublication, barriers.validateCohort(same_table_head, foreign));
    }
    const directory = try plan.participantDirectory();
    try std.testing.expectEqualDeep(try directory.digest(), prepared.head.participants_digest);
    var parsed = try parse(a, prepared.encoded);
    defer parsed.deinit();
    try parsed.value.requireProgress(a, prepared.head, prepared.owners);
    var forged_head = prepared.head;
    forged_head.participants_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, parsed.value.requireProgress(a, forged_head, prepared.owners));
    var forged = plan;
    var missing = [_]TablePair{plan.additional_tables[0]};
    missing[0].after.name = "wrong target";
    forged.additional_tables = &missing;
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, forged));
    forged = plan;
    forged.type_change.?.request.operation.add_enum.label = "unrequested";
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, forged));
    var damaged = directory;
    damaged.tables[2] = 9;
    try std.testing.expectError(error.InvalidGenerationPublication, damaged.digest());
}

test "schema validation publication type admission owns its compact intent and handles every preparation allocation failure" {
    const a = std.testing.allocator;
    var fixture = try TypeCohortFixture.init(a);
    defer fixture.deinit();
    const Check = struct {
        fn run(backing: A, proposed: Plan) !void {
            var prepared = try Prepared.init(backing, proposed);
            defer prepared.deinit();
            try std.testing.expect(prepared.type_request != null and prepared.type_after != null);
            try std.testing.expectEqualStrings("middle", prepared.type_request.?.value.operation.add_enum.label);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{fixture.value});
    var label = "middle".*;
    var plan = fixture.value;
    plan.type_change.?.request.operation.add_enum.label = &label;
    var prepared = try Prepared.init(a, plan);
    defer prepared.deinit();
    label[0] = 'X';
    try std.testing.expectEqualStrings("middle", prepared.type_request.?.value.operation.add_enum.label);
}

test "schema validation publication domain NOT NULL rejects kind substitution and mismatched owner purpose" {
    const a = std.testing.allocator;
    var enumeration = try TypeCohortFixture.init(a);
    defer enumeration.deinit();
    var forged = enumeration.value;
    forged.type_change.?.request.operation = .{ .set_domain_not_null = .{ .expected = .{ .id = 17, .generation = 1 } } };
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, forged));
    var domain = try TypeCohortFixture.initKind(a, true);
    defer domain.deinit();
    var prepared = try Prepared.init(a, domain.value);
    defer prepared.deinit();
    const owners = try a.dupe(OwnerIntent, domain.value.owners);
    defer a.free(owners);
    owners[1].binding.purpose = .enum_epoch;
    forged = domain.value;
    forged.owners = owners;
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, forged));
}

const SequenceCohortFixture = struct {
    arena: std.heap.ArenaAllocator,
    value: Plan,
    inputs: []const []const u8,
    pub fn init(backing: A) !@This() {
        var base = try TypeCohortFixture.init(backing);
        errdefer base.deinit();
        const a = base.arena.allocator();
        const sequences = local.system_catalog_column_sequences;
        const descriptor = try local.system_catalog_sequences.Descriptor.init(17, (try local.system_catalog_sequences.Record.create(.{})).definition);
        const change: SequenceChange = .{ .request = .{ .admission = .{ .head = 0, .nonce = @splat(12) }, .target = .{ .table = "shared_seq" }, .namespace_id = 2, .incarnation = @splat(0x11), .operation = .{ .drop = .{ .expected = .{ .id = 17, .generation = 1 }, .cascade = true } } }, .before = descriptor, .database_id = 1, .references = .{ .revision = 1, .count = 2 } };
        const pairs = try a.alloc(TablePair, 2);
        const intents = try a.dupe(OwnerIntent, base.value.owners);
        const inputs = try a.alloc([]const u8, 2);
        const selector = try std.json.Stringify.valueAlloc(a, change.selector(), .{});
        for (pairs, intents, inputs, 0..) |*pair, *intent, *input, i| {
            const table_id: u64 = 7 + @as(u64, @intCast(i));
            const binding: sequences.Binding = .{ .generation = .serial, .incarnation = @splat(0x11), .sequence_id = 17, .namespace_id = 2, .owner = .{ .table_id = table_id, .column_id = 1 }, .target = .{ .table = "shared_seq" } };
            var schema = try std.json.parseFromSliceLeaky(std.json.Value, a,
                \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
            , .{});
            try schema.object.put(a, "version", .{ .integer = if (i == 0) 1 else 4 });
            const defaults = try std.json.Stringify.valueAlloc(a, .{.{ .column = "n", .sequence = binding }}, .{});
            try schema.object.put(a, "column_defaults", try std.json.parseFromSliceLeaky(std.json.Value, a, defaults, .{}));
            pair.before = .{ .table_id = table_id, .name = if (i == 0) "first" else "second", .schema_json = try std.json.Stringify.valueAlloc(a, schema, .{}), .indexes_json = "{}", .schema_reader_requirements = .{ .semantic_version = 42, .encoding_version = 42 } };
            pair.after = pair.before;
            pair.after.schema_json = try change.selector().schemaAlloc(a, table_id, pair.before.schema_json);
            var before = try local.schema_mod.PreparedSchemaDefinition.init(a, pair.before.schema_json);
            defer before.deinit(a);
            var after = try local.schema_mod.PreparedSchemaDefinition.init(a, pair.after.schema_json);
            defer after.deinit(a);
            var catalog = try local.schema_relational_declarations.compileCatalog(a, before.public_schema, table_id, null);
            defer catalog.deinit();
            var successor = try local.schema_relational_declarations.compileCatalog(a, after.public_schema, table_id, catalog.value);
            defer successor.deinit();
            input.* = try contract.encodeInputs(a, .{ pair.before.schema_json, pair.after.schema_json, catalog.value, selector });
            intent.binding.purpose = .sequence_default_removal;
            intent.binding.columns = 1;
            intent.binding.before_version = before.runtime_schema.version;
            intent.binding.after_version = after.runtime_schema.version;
            intent.binding.before_public = contract.hash(pair.before.schema_json);
            intent.binding.after_public = contract.hash(pair.after.schema_json);
            intent.binding.before_schema = contract.hash(try local.storage_schema.serializeSchema(a, before.runtime_schema));
            intent.binding.after_schema = contract.hash(try local.storage_schema.serializeSchema(a, after.runtime_schema));
            intent.binding.fence.catalog_digest = contract.hash(catalog.value);
            intent.binding.after_catalog = contract.hash(successor.value);
            intent.binding.inputs = contract.hash(input.*);
            intent.binding.projection_identity = try change.selector().fingerprint();
        }
        const value: Plan = .{ .version = 4, .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .id = @splat(1), .before = pairs[0].before, .after = pairs[0].after, .additional_tables = pairs[1..], .owners = intents, .selection = .{ .sequence_default_removal = change.selector() }, .sequence_change = change };
        return .{ .arena = base.arena, .value = value, .inputs = inputs };
    }
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

test "schema validation publication sequence retirement binds exact default-only postimages immutable owner membership and SQL intent" {
    const a = std.testing.allocator;
    var fixture = try SequenceCohortFixture.init(a);
    defer fixture.deinit();
    var prepared = try Prepared.init(a, fixture.value);
    defer prepared.deinit();
    try std.testing.expect(prepared.type_request == null and prepared.type_after == null and prepared.sequence_request != null);
    try std.testing.expectEqual(@as(u16, 2), prepared.head.participant_count);
    try std.testing.expectEqual(@as(u64, 17), prepared.head.sequence_retirement.?.id);
    var parsed = try parse(a, prepared.encoded);
    defer parsed.deinit();
    try parsed.value.requireProgress(a, prepared.head, prepared.owners);
    var forged = fixture.value;
    forged.sequence_change.?.request.operation.drop.cascade = false;
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, forged));
    forged = fixture.value;
    forged.sequence_change.?.request.operation.drop.expected.?.generation += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, forged));
    forged = fixture.value;
    forged.after = forged.before;
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, forged));
    forged = fixture.value;
    forged.sequence_change.?.references.count = 0;
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, forged));
    const Faults = struct {
        fn run(backing: A, plan: Plan) !void {
            var result = try Prepared.init(backing, plan);
            defer result.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Faults.run, .{fixture.value});
}

test "schema validation publication rejects dependency retirement outside the owning table coordinator" {
    const a = std.testing.allocator;
    var fixture = try SequenceCohortFixture.init(a);
    defer fixture.deinit();
    var accepted = try Prepared.init(a, fixture.value);
    defer accepted.deinit();
    var uncoordinated = fixture.value;
    const sequence = fixture.value.sequence_change.?.selector();
    uncoordinated.selection = .{ .dependent_object_retirement = .{ .sequences = &.{sequence} } };
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, uncoordinated));
}

test "schema validation publication retirement cohort binds immutable root effects and withdraws only selected dependencies" {
    const a = std.testing.allocator;
    var fixture = try SequenceCohortFixture.init(a);
    defer fixture.deinit();
    const scratch = fixture.arena.allocator();
    const selected: local.system_catalog_dependency_retirement.Selection = .{
        .relations = &.{.{ .table_id = 91, .physical_name = "retiring" }},
        .sequences = &.{fixture.value.sequence_change.?.selector()},
    };
    var value = fixture.value;
    value.version = 5;
    value.sequence_change = null;
    value.selection = .{ .dependent_object_retirement = selected };
    value.retirement_root = .{ .root = .{ .group = 1, .table = 91, .id = @splat(3) }, .effects = .{ .root_table = 91, .digest = @splat(4), .survivors = 2, .counters = 1, .views = 0 }, .physical_plan = @splat(5) };
    const intents = try scratch.dupe(OwnerIntent, value.owners);
    value.owners = intents;
    for (intents, fixture.inputs) |*intent, input| {
        const parts = try contract.inputParts(input);
        const pair: TablePair = if (intent.range.table_id == value.before.table_id) .{ .before = value.before, .after = value.after } else value.additional_tables[0];
        var candidate = try local.storage_db_relational_nullability_job.Candidate.initDependencyRetirement(scratch, pair.before.table_id, pair.before.schema_json, pair.after.schema_json, parts[2], selected, .{});
        defer candidate.deinit();
        intent.binding.inputs = contract.hash(candidate.inputs);
        intent.binding.projection_identity = try selected.fingerprint();
        intent.binding.purpose = .dependent_object_retirement;
    }
    var prepared = try Prepared.init(a, value);
    defer prepared.deinit();
    try std.testing.expectEqualDeep(value.retirement_root, prepared.head.retirement_root);
    try std.testing.expectEqual(@as(u16, 2), prepared.head.participant_count);
    var decoded = try parse(a, prepared.encoded);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(value.retirement_root, decoded.value.retirement_root);
    var forged = value;
    forged.retirement_root = null;
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, forged));
    forged = value;
    forged.retirement_root.?.root.table += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, forged));
    forged = value;
    forged.retirement_root.?.effects.survivors = 1;
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, forged));
    forged = value;
    forged.retirement_root.?.physical_plan[0] ^= 1;
    try std.testing.expect(!std.mem.eql(u8, &try forged.digest(), &prepared.head.plan_digest));
    forged = value;
    forged.id = forged.retirement_root.?.root.id;
    try std.testing.expectError(error.InvalidGenerationPublication, Prepared.init(a, forged));
}
