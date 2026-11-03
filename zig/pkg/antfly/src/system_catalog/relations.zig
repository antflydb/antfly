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

//! Immutable relation definitions and annotations on the shared native journal.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const calls = @import("server_call.zig");
const catalog = @import("catalog_mutations.zig");
const contract = @import("../metadata/storage/raft_apply_contract.zig");
const tables = @import("../metadata/table_manager.zig");
const A = std.mem.Allocator;

pub const Intent = union(enum) {
    schema: calls.RelationSchemaMutation,
    description: calls.RelationDescriptionMutation,
    pub fn guard(self: @This()) domain.RelationMutationGuard {
        return switch (self) {
            inline else => |value| value.guard,
        };
    }
    pub fn action(self: @This()) domain.Action {
        return if (self == .schema) .alter else .comment;
    }
    pub fn digest(self: @This()) ![32]u8 {
        return catalog.canonicalDigest("antfly:sql-relation-intent:v1", self);
    }
    pub fn validate(self: @This()) !void {
        switch (self) {
            inline else => |value| try value.validate(),
        }
    }
};
pub const Prepared = struct {
    intent_digest: [32]u8,
    change: union(enum) {
        definition: struct { mode: enum { schema, description }, guard: domain.RelationMutationGuard, expected: [32]u8, table: tables.TableRecord },
        column: contract.ColumnDescriptionCommand,
        object: contract.ObjectDescriptionCommand,
    },
    pub fn guard(self: @This()) domain.RelationMutationGuard {
        return switch (self.change) {
            .object => |value| value.request.guard,
            inline else => |value| value.guard,
        };
    }
    pub fn action(self: @This()) domain.Action {
        return if (self.change == .definition and self.change.definition.mode == .schema) .alter else .comment;
    }
    pub fn validate(self: @This()) !void {
        try self.guard().validate();
        if (std.mem.allEqual(u8, &self.intent_digest, 0)) return error.InvalidCatalogMutation;
        switch (self.change) {
            .definition => |value| {
                if (value.guard.owner.kind != .table or value.table.table_id != value.guard.owner.table_id or std.mem.allEqual(u8, &value.expected, 0)) return error.InvalidCatalogMutation;
            },
            inline else => |value| try value.validate(),
        }
    }
};
pub fn mutation(guard: domain.RelationMutationGuard, action: domain.Action) domain.Mutation {
    return .{ .kind = .table, .action = action, .database = guard.target.database, .namespace = guard.target.namespace, .name = guard.logical_table, .table_id = guard.owner.table_id };
}
pub fn tableDigest(table: tables.TableRecord) ![32]u8 {
    return catalog.canonicalDigest("antfly:sql-relation-predecessor:v1", table);
}
pub fn prepare(a: A, store: anytype, group: u64, input: catalog.Preparation) ![]u8 {
    const intent = input.relation orelse return error.InvalidCatalogMutation;
    try intent.validate();
    if (input.view != null) return error.InvalidCatalogMutation;
    const guard = intent.guard();
    const label = mutation(guard, intent.action());
    if (!try catalog.matchesIntent(a, label, input.request.mutation)) return error.InvalidCatalogMutation;
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const prepared: Prepared = .{ .intent_digest = try intent.digest(), .change = change: {
        if (intent == .description) {
            const description = intent.description;
            if (description.object) |key| {
                const read: domain.ObjectAnnotationRead = .{ .guard = guard, .key = key };
                const bytes = try store.getSqlObjectAnnotation(scratch, group, read);
                const observed = try std.json.parseFromSliceLeaky(domain.object_annotation_store.Snapshot, scratch, bytes, .{});
                break :change .{ .object = .{ .request = read, .expected = .{ .epoch = observed.epoch, .object_id = observed.object.id }, .description = description.description } };
            }
            if (description.column) |name| {
                var observed = try store.getSqlColumnIdentities(scratch, group, guard);
                defer observed.deinit();
                const id = for (observed.value.columns) |column| {
                    if (std.mem.eql(u8, column.name, name)) break column.id;
                } else return error.UndefinedColumn;
                break :change .{ .column = .{ .guard = guard, .column = .{ .name = name, .value = description.description }, .column_id = id, .epoch = observed.value.epoch } };
            }
        }
        const before = (try store.getTable(scratch, group, guard.owner.table_id)) orelse return error.CatalogGenerationChanged;
        if (try @import("../api/tables.zig").schemaVersion(before.schema_json) != guard.owner.schema_version) return error.CatalogGenerationChanged;
        var after = if (intent == .schema) try @import("../api/tables.zig").applySchemaMutationRecord(scratch, &before, .replace, intent.schema.schema_json) else try tables.cloneTable(scratch, before);
        if (intent == .description) after.description = intent.description.description;
        const names = local.system_catalog_relation_names.native_indexes;
        if (after.sql_native_indexes_json.len != 0) after.sql_native_indexes_json = try names.prune(scratch, after.sql_native_indexes_json, after.indexes_json);
        try @import("../api/indexes.zig").validateArtifactEnrichmentsForTableIndexesJson(scratch, after.indexes_json);
        try local.inference_managed_embedder.validateEmbeddingProducerOwnershipJson(scratch, after.indexes_json);
        break :change .{ .definition = .{ .mode = if (intent == .schema) .schema else .description, .guard = guard, .expected = try tableDigest(before), .table = after } };
    } };
    try prepared.validate();
    const cut = try store.systemCatalogAdmission(scratch, group, label);
    if (!std.mem.eql(u8, &cut.incarnation, &guard.incarnation)) return error.CatalogGenerationChanged;
    const command: contract.SystemCatalogCommand = .{ .expected_revision = cut.meta.revision, .mutation = label, .sql_relation = prepared, .sql_catalog_admission = .{ .request = .{ .nonce = input.nonce, .head = cut.meta.sequence_outcome_head orelse 0, .command_fence = input.command_fence }, .incarnation = cut.incarnation } };
    try catalog.validate(command);
    const bytes = try std.json.Stringify.valueAlloc(a, command, .{});
    errdefer a.free(bytes);
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    return bytes;
}
