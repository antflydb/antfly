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

//! Immutable logical effects for owning-table retirement. No owner preparation
//! or mutation authority is inferred from this document. Native admission must
//! independently rejoin every indexed dependency and source preimage.
const std = @import("std");
const local = @import("antfly_local_sources");
const records = local.common_topology_records;
const domain = local.system_catalog_domain;
const tables = @import("table_manager.zig");
const sequences = @import("../system_catalog/sequence_catalog.zig");
const sequence_uses = @import("../system_catalog/sequence_default_dependencies.zig");
const fk_uses = @import("../system_catalog/foreign_key_dependencies.zig");
const views = @import("../system_catalog/views.zig");
const A = std.mem.Allocator;
pub const max_bytes = 16 * 1024 * 1024;
pub const max_survivors = 128;
pub const max_counters = 256;
pub const max_views = 256;
pub const max_edges = 16384;
pub const max_parents = 128;
pub const max_ranges = 4096;
pub const Selection = local.system_catalog_dependency_retirement.Selection;
pub const Relation = local.system_catalog_dependency_retirement.Relation;
pub const Pair = struct { before: records.TableRecord, after: records.TableRecord };
pub const Counter = struct {
    resource: domain.Resource,
    references: sequence_uses.Stamp,
    owner_column: u64,
};
pub const ViewEdge = views.DependencyKey;
pub const Backing = struct { table: records.TableRecord, ranges: []const records.RangeRecord };
/// The compact reference is suitable for a publication plan. The effects body
/// is persisted once; ACKs and physical preparation seals must not rewrite it.
pub const Reference = struct {
    digest: [32]u8,
    root_table: u64,
    survivors: u16,
    counters: u16,
    views: u16,
    pub fn validate(self: Reference) !void {
        if (self.root_table == 0 or std.mem.allEqual(u8, &self.digest, 0) or self.survivors > max_survivors or self.counters > max_counters or self.views > max_views) return error.InvalidTableRetirementManifest;
    }
};
pub const Manifest = struct {
    version: u8 = 1,
    metadata_group_id: u64,
    metadata_incarnation: local.metadata_incarnation.MetadataClusterIncarnation,
    guard: domain.RelationMutationGuard,
    root: records.TableRecord,
    root_ranges: []const records.RangeRecord,
    incoming: fk_uses.Stamp,
    selection: Selection,
    counters: []const Counter,
    survivors: []const Pair,
    dependent_views: []const domain.Resource,
    view_backings: []const Backing,
    survivor_ranges: []const records.RangeRecord,
    /// Distinct outgoing parents, including self/other survivors. Only parents
    /// outside the selected cohort need another owned table/range preimage.
    outgoing_parents: []const u64,
    external_parents: []const Backing,
    view_edges: []const ViewEdge,

    pub fn validate(self: Manifest, backing: A) !void {
        if (self.version != 1 or self.metadata_group_id == 0 or !local.metadata_incarnation.isValid(self.metadata_incarnation) or
            self.root.table_id == 0 or self.guard.owner.kind != .table or self.guard.owner.table_id != self.root.table_id or
            self.guard.constraint_name_admission != null or self.guard.object_renames.len != 0 or self.root.sql_materialized_view_id != null or
            self.counters.len > max_counters or self.survivors.len > max_survivors or self.dependent_views.len > max_views or
            self.view_backings.len > self.dependent_views.len or self.view_edges.len > max_edges or self.root.read_schema_json.len != 0 or self.root.storage_migration != null) return error.InvalidTableRetirementManifest;
        try self.guard.validate();
        if (!std.mem.eql(u8, &std.fmt.bytesToHex(self.guard.incarnation, .lower), &self.metadata_incarnation)) return error.MetadataIncarnationMismatch;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(self.root.schema_json, &digest, .{});
        if (!std.mem.eql(u8, &digest, &self.guard.owner.schema_digest)) return error.InvalidTableRetirementManifest;
        var root_version = try std.json.parseFromSlice(struct { version: u32 = 0 }, backing, if (self.root.schema_json.len == 0) "{}" else self.root.schema_json, .{ .ignore_unknown_fields = true });
        defer root_version.deinit();
        if (root_version.value.version != self.guard.owner.schema_version) return error.InvalidTableRetirementManifest;
        try self.selection.validate();
        if (self.selection.relations.len != 1 or self.selection.relations[0].table_id != self.root.table_id or
            !std.mem.eql(u8, self.selection.relations[0].physical_name, self.root.name) or self.selection.sequences.len != self.counters.len or
            (self.incoming.revision == 0 and self.incoming.count != 0) or self.incoming.active_count > self.incoming.count) return error.InvalidTableRetirementManifest;
        var previous: u64 = 0;
        for (self.counters, self.selection.sequences) |counter, selected| {
            var region = std.heap.ArenaAllocator.init(backing);
            defer region.deinit();
            const a = region.allocator();
            const resource = counter.resource;
            if (resource.kind != .sequence or resource.id <= previous or resource.parent_id != selected.namespace_id or
                resource.id != selected.sequence_id or !std.mem.eql(u8, &selected.incarnation, &self.guard.incarnation) or
                (counter.references.revision == 0 and counter.references.count != 0)) return error.InvalidTableRetirementManifest;
            const descriptor = try sequences.sequence.Descriptor.decode(a, resource.source_json);
            const definition = try descriptor.definition();
            const owner = definition.owned_by orelse return error.InvalidTableRetirementManifest;
            if (descriptor.id != resource.id or owner.table_id != self.root.table_id or owner.column_id != counter.owner_column) return error.InvalidTableRetirementManifest;
            previous = resource.id;
        }
        previous = 0;
        for (self.survivors) |pair| {
            if (pair.before.table_id <= previous or pair.before.table_id == self.root.table_id or pair.before.storage.engine != .local or
                pair.before.read_schema_json.len != 0 or pair.before.storage_migration != null or pair.before.sql_materialized_view_id != null) return error.InvalidTableRetirementManifest;
            var expected = try tables.cloneTable(backing, pair.before);
            defer tables.freeTable(backing, expected);
            const schema = try self.selection.schemaAlloc(backing, pair.before.table_id, pair.before.schema_json);
            backing.free(expected.schema_json);
            expected.schema_json = schema;
            if (!tables.tableDefinitionsEqual(expected, pair.after)) return error.InvalidTableRetirementManifest;
            previous = pair.before.table_id;
        }
        try self.validateParentsAndRanges(backing);
        // Count the actual logical withdrawals, not only listed table IDs.
        // Together with native indexed stamps this rejects an omitted borrower
        // or active-FK survivor without trusting a producer's claimed closure.
        const counts = try backing.alloc(u64, self.counters.len);
        defer backing.free(counts);
        @memset(counts, 0);
        var incoming_active: u64 = 0;
        try self.countFootprint(backing, self.root, counts, &incoming_active);
        for (self.survivors) |pair| try self.countFootprint(backing, pair.before, counts, &incoming_active);
        if (incoming_active != self.incoming.active_count or self.incoming.active_count > self.incoming.count) return error.InvalidTableRetirementManifest;
        for (self.counters, counts) |counter, count| if (counter.references.count != count) return error.InvalidTableRetirementManifest;
        // Each selected view must be reachable through an actual source path.
        // Build its reverse adjacency once instead of repeatedly decoding the
        // whole frontier. Completeness against storage remains a native check.
        var region = std.heap.ArenaAllocator.init(backing);
        defer region.deinit();
        const a = region.allocator();
        var indexes: std.AutoHashMapUnmanaged(u64, usize) = .empty;
        const view_records = try a.alloc(views.Record, self.dependent_views.len);
        previous = 0;
        for (self.dependent_views, view_records, 0..) |resource, *record, i| {
            if (resource.kind != .view or resource.id <= previous) return error.InvalidTableRetirementManifest;
            previous = resource.id;
            record.* = (try views.Record.decode(a, resource.source_json)).value;
            if (record.id != resource.id) return error.InvalidTableRetirementManifest;
            try indexes.put(a, resource.id, i);
        }
        const reachable = try a.alloc(bool, self.dependent_views.len);
        @memset(reachable, false);
        const outgoing = try a.alloc(std.ArrayList(usize), self.dependent_views.len);
        @memset(outgoing, .empty);
        var queue: std.ArrayList(usize) = .empty;
        for (view_records, 0..) |record, i| for (record.definition.sources) |source| {
            if (source.kind == .table and source.table_id == self.root.table_id and !reachable[i]) {
                reachable[i] = true;
                try queue.append(a, i);
            } else if (source.kind == .view) if (indexes.get(source.table_id)) |parent| try outgoing[parent].append(a, i);
        };
        var cursor: usize = 0;
        while (cursor < queue.items.len) : (cursor += 1) for (outgoing[queue.items[cursor]].items) |child| {
            if (reachable[child]) continue;
            reachable[child] = true;
            try queue.append(a, child);
        };
        if (queue.items.len != self.dependent_views.len) return error.InvalidTableRetirementManifest;
        var expected_edges: std.ArrayList(ViewEdge) = .empty;
        for (view_records) |record| for (try record.dependencies(a)) |dependency| try expected_edges.append(a, .{ .dependency = dependency, .view = record.id });
        std.mem.sort(ViewEdge, expected_edges.items, {}, edgeLess);
        if (expected_edges.items.len != self.view_edges.len) return error.InvalidTableRetirementManifest;
        for (expected_edges.items, self.view_edges) |expected, actual| if (!std.meta.eql(expected, actual)) return error.InvalidTableRetirementManifest;
        var expected_backings: usize = 0;
        var backing_ids: std.AutoHashMapUnmanaged(u64, void) = .empty;
        for (view_records, self.dependent_views) |record, resource| if (record.materialization) |materialization| {
            expected_backings += 1;
            const actual = for (self.view_backings) |item| {
                if (item.table.table_id == materialization.table_id) break item;
            } else return error.InvalidTableRetirementManifest;
            if (actual.table.sql_materialized_view_id != resource.id or !std.mem.eql(u8, actual.table.name, materialization.storage_name) or
                (try backing_ids.getOrPut(a, actual.table.table_id)).found_existing) return error.InvalidTableRetirementManifest;
        };
        if (expected_backings != self.view_backings.len) return error.InvalidTableRetirementManifest;
    }

    fn validateParentsAndRanges(self: Manifest, backing: A) !void {
        if (self.outgoing_parents.len > max_parents or self.external_parents.len > self.outgoing_parents.len) return error.InvalidTableRetirementManifest;
        var arena = std.heap.ArenaAllocator.init(backing);
        defer arena.deinit();
        const a = arena.allocator();
        var names: std.StringHashMapUnmanaged(u64) = .empty;
        var ids: std.AutoHashMapUnmanaged(u64, void) = .empty;
        try names.put(a, self.root.name, self.root.table_id);
        try ids.put(a, self.root.table_id, {});
        for (self.survivors) |pair| {
            if ((try names.getOrPut(a, pair.before.name)).found_existing or (try ids.getOrPut(a, pair.before.table_id)).found_existing) return error.InvalidTableRetirementManifest;
            names.getPtr(pair.before.name).?.* = pair.before.table_id;
        }
        var previous: u64 = 0;
        for (self.external_parents) |parent| {
            const table = parent.table;
            if (table.table_id <= previous or table.storage.engine != .local or table.read_schema_json.len != 0 or table.storage_migration != null or
                table.sql_materialized_view_id != null or (try ids.getOrPut(a, table.table_id)).found_existing) return error.InvalidTableRetirementManifest;
            const entry = try names.getOrPut(a, table.name);
            if (entry.found_existing) return error.InvalidTableRetirementManifest;
            entry.value_ptr.* = table.table_id;
            previous = table.table_id;
        }
        const schema = try std.json.parseFromSliceLeaky(struct { foreign_keys: ?[]const struct { parent_table: []const u8 } = null }, a, if (self.root.schema_json.len == 0) "{}" else self.root.schema_json, .{ .ignore_unknown_fields = true });
        var actual: std.AutoHashMapUnmanaged(u64, void) = .empty;
        for (schema.foreign_keys orelse &.{}) |fk| {
            const id = names.get(fk.parent_table) orelse return error.InvalidTableRetirementManifest;
            try actual.put(a, id, {});
        }
        if (actual.count() != self.outgoing_parents.len) return error.InvalidTableRetirementManifest;
        previous = 0;
        for (self.outgoing_parents) |id| {
            if (id <= previous or !actual.contains(id)) return error.InvalidTableRetirementManifest;
            previous = id;
        }
        for (self.external_parents) |parent| if (!actual.contains(parent.table.table_id)) return error.InvalidTableRetirementManifest;
        // A range can occur only once in this logical cohort. Overlapping
        // survivor/parent roles reuse the same preimage instead of duplicating
        // range lists in the manifest or growing ACK rewrites.
        var range_ids: std.AutoHashMapUnmanaged(u64, void) = .empty;
        try validateRanges(a, &range_ids, self.root_ranges, self.root.table_id);
        for (self.external_parents) |parent| try validateRanges(a, &range_ids, parent.ranges, parent.table.table_id);
        for (self.view_backings) |item| try validateRanges(a, &range_ids, item.ranges, item.table.table_id);
        var previous_table: u64 = 0;
        var previous_range: u64 = 0;
        for (self.survivor_ranges) |range| {
            if (range.table_id < previous_table or (range.table_id == previous_table and range.group_id <= previous_range)) return error.InvalidTableRetirementManifest;
            const found = for (self.survivors) |pair| {
                if (pair.before.table_id == range.table_id) break true;
            } else false;
            if (!found) return error.InvalidTableRetirementManifest;
            try validateRanges(a, &range_ids, &.{range}, range.table_id);
            previous_table = range.table_id;
            previous_range = range.group_id;
        }
    }
    fn validateRanges(a: A, ids: *std.AutoHashMapUnmanaged(u64, void), ranges: []const records.RangeRecord, table: u64) !void {
        var previous: u64 = 0;
        for (ranges) |range| {
            if (range.table_id != table or range.group_id <= previous or ids.count() == max_ranges or (try ids.getOrPut(a, range.group_id)).found_existing) return error.InvalidTableRetirementManifest;
            previous = range.group_id;
        }
    }

    fn countFootprint(self: Manifest, backing: A, table: records.TableRecord, counts: []u64, incoming: *u64) !void {
        var arena = std.heap.ArenaAllocator.init(backing);
        defer arena.deinit();
        const a = arena.allocator();
        const schema = try std.json.parseFromSliceLeaky(struct {
            foreign_keys: ?[]const struct { parent_table: []const u8 } = null,
            column_defaults: ?[]const struct { sequence: ?std.json.Value = null } = null,
        }, a, if (table.schema_json.len == 0) "{}" else table.schema_json, .{ .ignore_unknown_fields = true });
        for (schema.foreign_keys orelse &.{}) |fk| if (std.mem.eql(u8, fk.parent_table, self.root.name)) {
            incoming.* += 1;
            break;
        };
        for (schema.column_defaults orelse &.{}) |entry| if (entry.sequence) |value| {
            var parsed = try local.system_catalog_column_sequences.Binding.parse(backing, value);
            defer parsed.deinit();
            const binding = parsed.value;
            var low: usize = 0;
            var high = self.counters.len;
            while (low < high) {
                const middle = low + (high - low) / 2;
                if (self.counters[middle].resource.id < binding.sequence_id) low = middle + 1 else high = middle;
            }
            if (low == self.counters.len or self.counters[low].resource.id != binding.sequence_id) continue;
            if (binding.owner.table_id != table.table_id or binding.namespace_id != self.counters[low].resource.parent_id or !std.mem.eql(u8, &binding.incarnation, &self.guard.incarnation)) return error.InvalidTableRetirementManifest;
            counts[low] += 1;
        };
    }

    pub fn reference(self: Manifest) !Reference {
        if (self.survivors.len > max_survivors or self.counters.len > max_counters or self.dependent_views.len > max_views) return error.SqlProgramLimitExceeded;
        var count = std.Io.Writer.Discarding.init(&.{});
        try std.json.Stringify.value(self, .{}, &count.writer);
        if (count.fullCount() > max_bytes) return error.SqlProgramLimitExceeded;
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(std.crypto.hash.Blake3.init(.{}), &.{});
        try writer.writer.writeAll("antfly/table-retirement-effects/v1\x00");
        try std.json.Stringify.value(self, .{}, &writer.writer);
        var digest: [32]u8 = undefined;
        writer.hasher.final(&digest);
        const result: Reference = .{ .digest = digest, .root_table = self.root.table_id, .survivors = @intCast(self.survivors.len), .counters = @intCast(self.counters.len), .views = @intCast(self.dependent_views.len) };
        try result.validate();
        return result;
    }
    pub fn encodeAlloc(self: Manifest, a: A) ![]u8 {
        var count = std.Io.Writer.Discarding.init(&.{});
        try std.json.Stringify.value(self, .{}, &count.writer);
        if (count.fullCount() > max_bytes) return error.SqlProgramLimitExceeded;
        const bytes = try a.alloc(u8, @intCast(count.fullCount()));
        errdefer a.free(bytes);
        var writer = std.Io.Writer.fixed(bytes);
        try std.json.Stringify.value(self, .{}, &writer);
        return bytes;
    }
};

pub const Owned = struct {
    arena: std.heap.ArenaAllocator,
    value: Manifest,
    pub fn parse(backing: A, bytes: []const u8) !Owned {
        if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidTableRetirementManifest;
        var arena = std.heap.ArenaAllocator.init(backing);
        errdefer arena.deinit();
        const value = try std.json.parseFromSliceLeaky(Manifest, arena.allocator(), bytes, .{ .allocate = .alloc_always });
        try value.validate(backing);
        return .{ .arena = arena, .value = value };
    }
    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn edgeLess(_: void, left: ViewEdge, right: ViewEdge) bool {
    if (left.dependency.kind != right.dependency.kind) return @backingInt(left.dependency.kind) < @backingInt(right.dependency.kind);
    if (left.dependency.id != right.dependency.id) return left.dependency.id < right.dependency.id;
    if (left.dependency.column != right.dependency.column) return left.dependency.column < right.dependency.column;
    return left.view < right.view;
}
