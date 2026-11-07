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

//! Definition-only SQL copying uses current grants and immutable source owners.
//! Publication remains the existing native table/catalog transaction.
const std = @import("std");
const local = @import("antfly_local_sources");
const ast = local.sql_ast;
const catalog = local.sql_catalog;
const domain = local.system_catalog_domain;
const Adapter = @import("sql_execution.zig").Adapter;
const auth = @import("sql_authorization_runtime.zig");
const A = std.mem.Allocator;
const Json = std.json.Value;
pub fn prepare(raw: *anyopaque, a: A, input: ast.CreateTable, limits: catalog.DefinitionCopyLimits) !catalog.CreateTableDefinition {
    const root: *Adapter = @ptrCast(@alignCast(raw));
    try root.context.ensureActive();
    if (input.like_tables.len == 0 or input.like_tables.len > 16 or input.persistence != .permanent or input.document != null) return error.UnsupportedSqlExecution;
    const destination: domain.Target = .{ .database = input.table.database orelse root.database, .namespace = input.table.namespace orelse root.namespace, .table = input.table.table };
    try @import("sql_catalog_auth.zig").authorizeTarget(root.server, root.identity.*, a, destination);
    // Conditional creation must not look up or authorize missing sources when
    // a committed destination already exists. Pending names are not absence.
    const destination_lookup: domain.ResolveMany = .{ .relations = &.{.{ .database = destination.database, .namespace = destination.namespace, .name = destination.table }}, .expected_revision = root.revision };
    var private = root.context;
    private.setting_admin = true;
    const destination_bytes = try root.server.source.systemCatalog(a, private, .{ .resolve_many = destination_lookup });
    const destination_cut = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, destination_bytes, .{ .allocate = .alloc_always });
    try destination_cut.validateRelations(destination_lookup);
    if (destination_cut.relations[0] != null) {
        if (!input.if_not_exists) return error.CatalogAlreadyExists;
        return .{ .name = input.table, .schema_json = "", .if_not_exists = true, .existing = true };
    }
    root.revision = destination_cut.revision;
    var create = input;
    const needs_types = for (create.columns) |column| {
        if (column.named_type != null) break true;
    } else false;
    if (needs_types) create = try @import("sql_type_schema.zig").bindDeclaredColumns(root, a, create);
    const backend = root.backend();
    const old_rights = root.table_privileges;
    defer root.table_privileges = old_rights;
    root.table_privileges = local.system_catalog_privileges.Access.bit(.select);
    const definitions = try a.alloc(Json, create.like_tables.len);
    var witnesses: std.ArrayList(domain.RelationMutationGuard) = .empty;
    const source_tables = try a.alloc(catalog.Table, create.like_tables.len);
    const source_targets = try a.alloc(domain.RelationTarget, create.like_tables.len);
    var bound: std.ArrayList(ast.Column) = .empty;
    var sequence_intents: std.ArrayList(local.system_catalog_column_sequences.Creation) = .empty;
    var comment_sources: std.ArrayList(u64) = .empty;
    var next_source: usize = 0;
    var source_bytes: usize = 0;
    for (0..create.columns.len + 1) |offset| {
        while (next_source < create.like_tables.len and create.like_tables[next_source].column_offset == offset) : (next_source += 1) {
            try root.context.ensureActive();
            const intent = create.like_tables[next_source];
            const table = try backend.vtable.resolve(backend.ptr, a, intent.source, .read);
            try auth.authorize(root, a, table);
            if (table.scope == null or table.schema_json.len == 0 or table.external_base_source != null) return error.UnsupportedSqlShape;
            source_bytes = std.math.add(usize, source_bytes, table.schema_json.len) catch return error.SqlProgramLimitExceeded;
            if (source_bytes > limits.bytes) return error.SqlProgramLimitExceeded;
            if (limits.checkpoint) |check| try check(limits.checkpoint_context);
            const scope = table.scope.?;
            source_tables[next_source] = table;
            if (intent.options.comments) {
                const repeated = for (comment_sources.items) |id| {
                    if (id == table.id) break true;
                } else false;
                if (!repeated) try comment_sources.append(a, table.id);
            }
            source_targets[next_source] = .{ .database = scope.database, .namespace = scope.namespace, .name = scope.name };
            definitions[next_source] = try std.json.parseFromSliceLeaky(Json, a, table.schema_json, .{ .parse_numbers = false });
            // The bound source descriptors carry immutable domain/enum views;
            // use those for validating expressions declared beside LIKE.
            const definition = definitions[next_source];
            const row = definition.object.get("document_schemas").?.object.get(definition.object.get("default_type").?.string).?.object.get("schema").?;
            const properties = row.object.get("properties").?;
            if (properties != .object) return error.InvalidSqlBackendResponse;
            for (properties.object.keys()) |name| {
                const column = try table.column(name);
                if (column.sequence_default) |binding| {
                    const copy_sequence = if (binding.generation == .serial) intent.options.defaults else intent.options.identity;
                    if (copy_sequence) {
                        if (binding.owner.table_id != table.id) return error.CatalogGenerationChanged;
                        const integer: @import("../system_catalog/sequence_catalog.zig").sequence.Integer = switch (column.element_type orelse .int64) {
                            .int16 => .int16,
                            .int32 => .int32,
                            .int64 => .int64,
                            else => return error.InvalidSequenceDefinition,
                        };
                        try sequence_intents.append(a, .{ .column = name, .generation = binding.generation, .options = .{ .integer = integer }, .source = .{ .table_id = table.id, .column_id = binding.owner.column_id } });
                    }
                }
                try bound.append(a, .{ .name = name, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .enum_view = column.enum_view, .domain_view = column.domain_view, .nullable = column.nullable });
            }
        }
        if (offset == create.columns.len) break;
        try bound.append(a, create.columns[offset]);
        if (create.columns[offset].sequence) |sequence| try sequence_intents.append(a, sequence);
    }
    if (next_source != create.like_tables.len) return error.InvalidSqlBackendResponse;
    // One bounded guard cut validates every source, rather than paying a
    // second metadata round trip for each independently authorized binding.
    const lookup: domain.ResolveMany = .{ .relations = source_targets, .expected_revision = root.revision, .expected_relation_epoch = destination_cut.relation_epoch };
    const bytes = try root.server.source.systemCatalog(a, private, .{ .resolve_many = lookup });
    const cut = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
    try cut.validateRelations(lookup);
    if (cut.revision != destination_cut.revision) return error.CatalogGenerationChanged;
    for (cut.relations, source_tables, source_targets) |found, table, target| {
        const relation = found orelse return error.CatalogGenerationChanged;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(table.schema_json, &digest, .{});
        if (relation.owner.kind != .table or relation.owner.table_id != table.id or relation.owner.schema_version != table.schema_version or
            !std.mem.eql(u8, &digest, &relation.owner.schema_digest) or !std.mem.eql(u8, relation.table.name, table.physical_name)) return error.CatalogGenerationChanged;
        const witness: domain.RelationMutationGuard = .{ .target = target, .logical_table = relation.logical_table, .owner = relation.owner, .incarnation = cut.relation_epoch.?.incarnation };
        const repeated = for (witnesses.items) |prior| {
            if (prior.owner.table_id == witness.owner.table_id) break true;
        } else false;
        if (!repeated) try witnesses.append(a, witness);
    }
    var validation = create;
    validation.columns = bound.items;
    validation.like_tables = &.{};
    try @import("sql_persisted_expressions.zig").validate(root, a, .{ .create_table = validation });
    const schema = try local.sql_ddl_runtime.createSchemaFromDefinitionsAndSequencesAlloc(a, create, definitions, sequence_intents.items, limits);
    const value = try std.json.parseFromSliceLeaky(Json, a, schema, .{});
    var object_comments: std.ArrayList(domain.object_annotations.Copy) = .empty;
    var offsets = [_]u32{ 0, 0, 0 };
    for (definitions, create.like_tables, source_tables) |definition, intent, table| {
        inline for (.{ "checks", "unique_constraints", "relational_indexes" }, 0..) |family, family_index| {
            const enabled = if (family_index == 0) intent.options.constraints else intent.options.indexes;
            if (enabled) if (definition.object.get(family)) |entries| {
                for (entries.array.items, 0..) |entry, ordinal| if (intent.options.comments) {
                    const name = entry.object.get("name").?.string;
                    const is_index = family_index == 2;
                    const index_owned = if (entry.object.get("origin")) |origin| origin == .string and std.mem.eql(u8, origin.string, "index") else false;
                    if (is_index or (family_index == 0 and !index_owned)) try object_comments.append(a, .{
                        .source_table_id = table.id,
                        .source = .{ .kind = if (is_index) .index else .constraint, .name = name },
                        .destination_family = @fromBackingInt(@intCast(family_index)),
                        .destination_ordinal = offsets[family_index] + @as(u32, @intCast(ordinal)),
                    });
                    // A UNIQUE rule owns a separate index object. When the
                    // ordered-index slot also exists it already carries that
                    // object's source annotation, so never assign it twice.
                    if (family_index == 1) {
                        const ordered = if (definition.object.get("relational_indexes")) |indexes| for (indexes.array.items) |index| {
                            if (std.mem.eql(u8, index.object.get("name").?.string, name)) break true;
                        } else false else false;
                        if (!ordered) try object_comments.append(a, .{
                            .source_table_id = table.id,
                            .source = .{ .kind = .index, .name = name },
                            .destination_family = .unique_constraints,
                            .destination_ordinal = offsets[family_index] + @as(u32, @intCast(ordinal)),
                        });
                    }
                };
                offsets[family_index] += @intCast(entries.array.items.len);
            };
        }
    }
    var generated: std.ArrayList([]const u8) = .empty;
    for (create.constraints) |constraint| if (constraint.generatedConstraintName()) |name| try generated.append(a, name);
    // Copied index/UNIQUE names are generated destination declarations, never
    // copied namespace ownership. Native admission resolves outside collisions.
    for ([_][]const u8{ "unique_constraints", "relational_indexes" }) |key| {
        if (value.object.get(key)) |entries| for (entries.array.items) |entry| {
            const name = entry.object.get("name").?.string;
            const explicit = for (create.constraints) |constraint| {
                const original: ?[]const u8 = switch (constraint) {
                    .add_unique => |rule| rule.name,
                    else => null,
                };
                if (original) |label| if (std.mem.eql(u8, label, name)) break true;
            } else false;
            const repeated = for (generated.items) |label| {
                if (std.mem.eql(u8, label, name)) break true;
            } else false;
            if (!explicit and !repeated) try generated.append(a, name);
        };
    }
    try root.context.ensureActive();
    return .{ .name = create.table, .schema_json = schema, .if_not_exists = create.if_not_exists, .tablespace = create.tablespace, .column_sequences = sequence_intents.items, .generated_constraint_names = generated.items, .creation_sources = witnesses.items, .creation_comment_sources = comment_sources.items, .creation_object_comments = object_comments.items, .creation_revision = root.revision };
}
