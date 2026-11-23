// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Native SQL partition membership ingress. Names are captured/authorized once;
//! metadata independently rejoins both owners, exact schemas and complete range
//! cover at admission and publication. Owner preparation is not publication.
const std = @import("std");
const local = @import("antfly_local_sources");
const ast = local.sql_ast;
const catalog = local.sql_catalog;
const domain = local.system_catalog_domain;
const partitions = @import("../../../metadata/schema_validation/plan.zig");
const tables = @import("../../../metadata/table_manager.zig");
const http = @import("../../http_server.zig");
const operation = local.api_operation;
const schema_partitions = local.schema_table_partitions;
const family = local.system_catalog_table_partitions;

/// Drive one bounded step using the ordinary native owner supervisor.
pub fn advanceBirth(server: *http.ApiHttpServer, a: std.mem.Allocator, input: operation.RequestContext, work: @import("../../../metadata/fk_generation_publication.zig").InitialWork) !bool {
    const publication = @import("../../../metadata/fk_generation_publication.zig");
    var context = input;
    context.schema_validation_authority = true;
    const encoded = try server.source.systemCatalog(a, context, .{ .fk_initial_create_status = work.child_table_id });
    const initial = try std.json.parseFromSliceLeaky(publication.InitialPublication, a, encoded, .{ .allocate = .alloc_always });
    try initial.validateState(a);
    if (initial.revision != work.revision or !std.mem.eql(u8, &initial.plan.id, &work.plan_id)) return false;
    const birth = initial.plan.partition_birth orelse return error.InvalidGenerationPublication;
    switch (work.target.partition) {
        .prepare => {
            if (initial.partition_journal != null) return false;
            const io = server.sharedApiIo() orelse return error.UnsupportedSqlExecution;
            const status = try server.source.status();
            const incarnation = status.metadata_incarnation orelse return error.MetadataIncarnationUnavailable;
            if (initial.plan.expected_incarnation) |expected| {
                if (!std.mem.eql(u8, &expected, &incarnation)) return error.MetadataIncarnationMismatch;
            }
            var transport: @import("../../schema_validation/owner_transport.zig").Transport = .{ .server = server, .alloc = a };
            defer transport.deinit();
            const cut: @import("../../../metadata/schema_validation/publication.zig").OwnerCut = .{
                .metadata_group_id = status.metadata_group_id,
                .metadata_incarnation = incarnation,
                .authority = if (server.cfg.deployment_mode == .standalone) .native else .raft,
                .ranges = birth.parent_ranges,
                .native_tables = if (birth.parent_before.sql_persistence != null) &.{birth.parent_before.table_id} else &.{},
            };
            var payload = try @import("../../schema_validation/plan_builder.zig").buildInitialPartition(&transport, a, io, context, cut, initial);
            defer payload.deinit();
            _ = try server.source.systemCatalog(a, context, .{ .schema_validation_submit = payload.command });
            return false;
        },
        .reconcile => {
            const lookup = initial.partition_journal orelse return error.GenerationPublicationChanged;
            const receipt = @import("../../../metadata/schema_validation/receipt.zig");
            const request: receipt.Request = .{ .lookup = lookup, .expected = .admission };
            const response = try server.source.systemCatalog(a, context, .{ .schema_validation_receipt_read = request });
            const observation = try std.json.parseFromSliceLeaky(receipt.Observation, a, response, .{ .allocate = .alloc_always });
            if (!try observation.isAcknowledged(request)) return false;
            const head = observation.head orelse return error.GenerationPublicationChanged;
            return head.phase == .installed or head.phase == .canceled;
        },
    }
}

pub fn bound(value: ast.PartitionBound) !family.Bound {
    return switch (value) {
        .minvalue => .minvalue,
        .maxvalue => .maxvalue,
        .expression => |expression| blk: {
            if (expression.* != .literal or expression.literal != .string) return error.UnsupportedSqlShape;
            const parsed = try local.sql_temporal_text.parseInput(.timestamp_tz, expression.literal.string);
            break :blk .{ .value = parsed.timestamp_tz.micros };
        },
    };
}

pub fn execute(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, input_context: operation.RequestContext, database: []const u8, namespace: []const u8, allocators: catalog.DdlAllocators, ddl: ast.CatalogDdl, path: ?*const @import("../../../pgwire/search_path.zig").Path, expected: ?@import("ddl_binding.zig").Guard) !catalog.DdlOutcome {
    const intent = ddl.partition orelse return error.InvalidSqlSyntax;
    var region = std.heap.ArenaAllocator.init(allocators.scratch);
    defer region.deinit();
    const a = region.allocator();
    const parent_target: domain.Target = .{ .database = ddl.name.database orelse database, .namespace = ddl.name.namespace orelse namespace, .table = ddl.name.table };
    const parent = (try @import("catalog.zig").captureTableAdmission(server, identity, input_context, a, parent_target, ddl.name.namespace != null or ddl.name.database != null, path)) orelse return error.TableNotFound;
    const child_name = switch (intent) {
        .attach => |attach| attach.child,
        .detach => |child| child,
    };
    const child_target: domain.Target = .{ .database = child_name.database orelse database, .namespace = child_name.namespace orelse namespace, .table = child_name.table };
    const child = (try @import("catalog.zig").captureTableAdmission(server, identity, input_context, a, child_target, child_name.namespace != null or child_name.database != null, path)) orelse return error.TableNotFound;
    if (parent.table.table_id == child.table.table_id) return error.SqlPartitionCycle;
    if (expected) |bound_guard| {
        const objects = try bound_guard.objects(a);
        const parent_pin = try objects.table(2, 0);
        const child_pin = try objects.table(2, 1);
        if (parent_pin.id != parent.table.table_id or child_pin.id != child.table.table_id or
            parent_pin.schema_version != parent.guard.owner.schema_version or child_pin.schema_version != child.guard.owner.schema_version or
            !std.mem.eql(u8, parent_pin.namespace, parent.guard.target.namespace) or
            !std.mem.eql(u8, child_pin.namespace, child.guard.target.namespace)) return error.CatalogGenerationChanged;
    }
    if (!std.mem.eql(u8, parent.guard.target.database, child.guard.target.database)) return error.InvalidCatalogMutation;
    const parent_definition = parent.table.query_definition orelse return error.CatalogGenerationChanged;
    const child_definition = child.table.query_definition orelse return error.CatalogGenerationChanged;
    var parsed_parent = try local.schema_mod.parseValidatedTableSchema(a, parent_definition.schema_json);
    defer parsed_parent.deinit(a);
    const state = (parsed_parent.sql_partition orelse return error.SqlWrongObjectType).value;
    if (state != .parent) return error.SqlWrongObjectType;
    const before = state.parent;
    if (before.parent.table_id != parent.table.table_id or before.parent.schema_version != parent.guard.owner.schema_version) return error.PartitionCatalogChanged;
    if (parsed_parent.foreign_keys != null) return error.UnsupportedSqlExecution;
    var inherited: std.ArrayList(family.ConstraintBinding) = .empty;
    if (intent == .attach) {
        try requireExistingConstraints(a, parent_definition.schema_json, child_definition.schema_json, &inherited);
        var parent_validator = try server.sql_schema_cache.pinValidator(server.sqlPlanCacheIo(), allocators.scratch, parent_definition.schema_json);
        defer parent_validator.deinit();
        var child_validator = try server.sql_schema_cache.pinValidator(server.sqlPlanCacheIo(), allocators.scratch, child_definition.schema_json);
        defer child_validator.deinit();
        try @import("../execution/partition_checks.zig").appendInherited(a, &inherited, parent_validator.validator(), child_validator.validator());
        try family.validateInheritance(inherited.items);
    }
    const bounds: family.Range = switch (intent) {
        .attach => |attach| .{ .lower = try bound(attach.bounds.lower), .upper = try bound(attach.bounds.upper) },
        .detach => for (before.members) |member| {
            if (member.relation.table_id == child.table.table_id) break member.bounds;
        } else return error.SqlNotPartition,
    };
    try bounds.validate();
    const child_before: family.Relation = .{ .table_id = child.table.table_id, .schema_version = child.guard.owner.schema_version, .definition = try schema_partitions.definitionDigest(a, child_definition.schema_json) };
    var child_after = child_before;
    child_after.schema_version = std.math.add(u32, child_before.schema_version, 1) catch return error.SqlLimitExceeded;
    var after = switch (intent) {
        .attach => try before.attach(a, .{ .relation = child_after, .bounds = bounds, .inherited = inherited.items }),
        .detach => try before.detach(a, child_before.table_id),
    };
    after.parent.schema_version = std.math.add(u32, before.parent.schema_version, 1) catch return error.SqlLimitExceeded;
    const transition: schema_partitions.Transition = .{ .operation = if (intent == .attach) .attach else .detach, .inherited = inherited.items, .before = before, .after = after, .child_before = child_before, .child_after = child_after, .bounds = bounds };
    const guards: partitions.PartitionGuards = .{ .parent = parent.guard, .child = child.guard };
    var snapshot = (try server.source.adminSnapshot()) orelse return error.CatalogGenerationChanged;
    defer server.source.freeAdminSnapshot(&snapshot);
    var pair: [2]tables.TableRecord = undefined;
    for ([_]u64{ parent.table.table_id, child.table.table_id }, &pair) |id, *record| record.* = for (snapshot.tables) |table| {
        if (table.table_id == id) break table;
    } else return error.CatalogGenerationChanged;
    if (!std.mem.eql(u8, pair[0].schema_json, parent_definition.schema_json) or !std.mem.eql(u8, pair[1].schema_json, child_definition.schema_json)) return error.CatalogGenerationChanged;
    std.mem.sort(tables.TableRecord, &pair, {}, struct {
        fn less(_: void, left: tables.TableRecord, right: tables.TableRecord) bool {
            return left.table_id < right.table_id;
        }
    }.less);
    var ranges: std.ArrayList(tables.RangeRecord) = .empty;
    for (snapshot.ranges) |range| if (range.table_id == parent.table.table_id or range.table_id == child.table.table_id) try ranges.append(a, range);
    std.mem.sort(tables.RangeRecord, ranges.items, {}, struct {
        fn less(_: void, left: tables.RangeRecord, right: tables.RangeRecord) bool {
            if (left.table_id != right.table_id) return left.table_id < right.table_id;
            return std.mem.order(u8, left.start_key, right.start_key) == .lt;
        }
    }.less);
    var native: std.ArrayList(u64) = .empty;
    for (pair) |table| if (table.sql_persistence != null) try native.append(a, table.table_id);
    var private = try input_context.platformDeadline();
    private.setting_admin = true;
    private.schema_validation_authority = true;
    const io = server.sharedApiIo() orelse return error.UnsupportedSqlExecution;
    var transport: @import("../../schema_validation/owner_transport.zig").Transport = .{ .server = server, .alloc = allocators.scratch };
    defer transport.deinit();
    const activation: @import("../../../metadata/schema_validation/publication.zig").OwnerCut = .{ .metadata_group_id = snapshot.status.metadata_group_id, .metadata_incarnation = snapshot.status.metadata_incarnation orelse return error.MetadataIncarnationUnavailable, .authority = if (server.cfg.deployment_mode == .standalone) .native else .raft, .ranges = ranges.items, .native_tables = native.items };
    var owned = @import("../../schema_validation/plan_builder.zig").buildPartition(&transport, allocators.scratch, io, private, activation, &pair, transition, guards) catch |err| return partitionAdmissionError(err);
    var transferred = false;
    defer if (!transferred) owned.deinit();
    const result = allocators.result;
    const anchor = if (owned.lookup.table == parent.table.table_id) parent.guard else child.guard;
    const anchor_version = if (owned.lookup.table == parent.table.table_id) after.parent.schema_version else child_after.schema_version;
    var receipt = try @import("catalog.zig").newReceipt(result, .{ .database = anchor.target.database, .namespace = anchor.target.namespace, .table = anchor.logical_table }, owned.lookup.table, anchor_version);
    errdefer {
        result.free(receipt.database);
        result.free(receipt.namespace);
        result.free(receipt.table);
        result.free(receipt.table_id);
        if (receipt.schema_validation_token) |token| result.free(token);
    }
    receipt.schema_validation_token = try result.dupe(u8, &owned.recovery_token);
    transferred = true;
    return @import("../../schema_validation/admission.zig").submitOwned(server.source, allocators.scratch, io, private, &owned, receipt, allocators.completion) catch |err| return partitionAdmissionError(err);
}

fn partitionAdmissionError(err: anyerror) anyerror {
    return switch (err) {
        // Owner-cut preparation does not submit a mutation. Once submitted,
        // admission returns these errors only for a proven rejection. Unknown
        // admission keeps its recovery token and is never safe to replay.
        error.GenerationPublicationChanged, error.MetadataIncarnationMismatch => error.CatalogPublicationProofPending,
        else => err,
    };
}

/// An already enforced equivalent child owner may be reused. Missing inherited
/// owners must first be built by the coordinated initial/index lifecycle; this
/// function never substitutes descriptor metadata for constraint activation.
fn requireExistingConstraints(a: std.mem.Allocator, parent_json: []const u8, child_json: []const u8, inherited: *std.ArrayList(family.ConstraintBinding)) !void {
    var parent = try local.schema_mod.PreparedSchemaDefinition.init(a, parent_json);
    defer parent.deinit(a);
    var child = try local.schema_mod.PreparedSchemaDefinition.init(a, child_json);
    defer child.deinit(a);
    const parent_columns = try catalog.relationalColumnsLeaky(a, parent.runtime_schema);
    const child_columns = try catalog.relationalColumnsLeaky(a, child.runtime_schema);
    if (parent_columns.len != child_columns.len) return error.SqlPartitionSchemaMismatch;
    const parent_generated = try schema_partitions.generatedNames(a, parent.public_schema);
    const child_generated = try schema_partitions.generatedNames(a, child.public_schema);
    var columns: std.StringHashMapUnmanaged(catalog.Column) = .empty;
    for (child_columns) |column| try columns.put(a, column.name, column);
    for (parent_columns) |column| {
        const candidate = columns.get(column.name) orelse return error.SqlPartitionSchemaMismatch;
        if (parent_generated.contains(column.name) != child_generated.contains(column.name)) return error.SqlPartitionSchemaMismatch;
        if (!std.mem.eql(u8, &try column.identitySignature(a), &try candidate.identitySignature(a)) or (!column.nullable and candidate.nullable)) return error.SqlPartitionSchemaMismatch;
    }
    const parent_definitions = try local.schema_relational_declarations.definitionFingerprints(a, parent.public_schema, parent.runtime_schema);
    defer local.schema_relational_declarations.freeDefinitions(a, parent_definitions);
    const child_definitions = try local.schema_relational_declarations.definitionFingerprints(a, child.public_schema, child.runtime_schema);
    defer local.schema_relational_declarations.freeDefinitions(a, child_definitions);
    // Canonicalize each declaration once. A wide family must not parse and
    // allocate every child's predicate again for each parent constraint.
    const Definition = local.storage_db_relational_integrity_catalog.Definition;
    var candidates: std.StringHashMapUnmanaged(std.ArrayList(Definition)) = .empty;
    for (child_definitions) |definition| {
        const key = try declarationKey(a, definition);
        const bucket = try candidates.getOrPut(a, key);
        if (!bucket.found_existing) bucket.value_ptr.* = .empty;
        try bucket.value_ptr.append(a, definition);
    }
    for (parent_definitions) |required| {
        const key = try declarationKey(a, required);
        const bucket = candidates.getPtr(key) orelse return error.SqlPartitionSchemaMismatch;
        if (bucket.items.len == 0) return error.SqlPartitionSchemaMismatch;
        const position = for (bucket.items, 0..) |candidate, index| {
            if (std.mem.eql(u8, candidate.name, required.name)) break index;
        } else 0;
        const found = bucket.orderedRemove(position);
        const kind: @FieldType(family.ConstraintBinding, "kind") = switch (required.kind) {
            .unique => .unique,
            .foreign_key => .foreign_key,
        };
        try inherited.append(a, .{ .kind = kind, .parent_name = try a.dupe(u8, required.name), .child_name = try a.dupe(u8, found.name) });
    }
}

fn declarationKey(a: std.mem.Allocator, definition: local.storage_db_relational_integrity_catalog.Definition) ![]const u8 {
    var value = try std.json.parseFromSliceLeaky(std.json.Value, a, definition.payload, .{ .parse_numbers = false });
    _ = value.object.orderedRemove("name");
    const canonical = try std.json.Stringify.valueAlloc(a, value, .{});
    return std.fmt.allocPrint(a, "{d}:{s}", .{ @backingInt(definition.kind), canonical });
}

test "SQL partition parent routing classifies only definite stale admission for retry" {
    try std.testing.expectEqual(error.CatalogPublicationProofPending, partitionAdmissionError(error.GenerationPublicationChanged));
    try std.testing.expectEqual(error.CatalogPublicationProofPending, partitionAdmissionError(error.MetadataIncarnationMismatch));
    try std.testing.expectEqual(error.MetadataMutationOutcomeUnknown, partitionAdmissionError(error.MetadataMutationOutcomeUnknown));
    try std.testing.expectEqual(error.SqlNotPartition, partitionAdmissionError(error.SqlNotPartition));
}

test "SQL partition parent routing attach persists inherited declarations and native CHECK enforcement" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const parent_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"parent_key","columns":["at"]}],"checks":[{"name":"positive","column":"n","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"at":{"type":"datetime"},"n":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const child_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"leaf_key","columns":["at"]}],"checks":[{"name":"positive","column":"n","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"},"at":{"type":"datetime"}},"additionalProperties":false}}}}
    ;
    var inherited: std.ArrayList(family.ConstraintBinding) = .empty;
    try requireExistingConstraints(scratch, parent_json, child_json, &inherited);
    var parent = try local.schema_mod.CompiledTableValidator.init(a, parent_json);
    defer parent.deinit(a);
    var child = try local.schema_mod.CompiledTableValidator.init(a, child_json);
    defer child.deinit(a);
    try @import("../execution/partition_checks.zig").appendInherited(scratch, &inherited, parent, child);
    try family.validateInheritance(inherited.items);
    try std.testing.expectEqual(@as(usize, 2), inherited.items.len);
    try std.testing.expectEqualStrings("parent_key", inherited.items[0].parent_name);
    try std.testing.expectEqualStrings("leaf_key", inherited.items[0].child_name);
    const descriptor: family.Descriptor = .{ .parent = .{ .table_id = 7, .schema_version = 1, .definition = try schema_partitions.definitionDigest(scratch, parent_json) }, .revision = 1, .key_attribute = 1, .key_name = "at" };
    const before: family.Relation = .{ .table_id = 9, .schema_version = 1, .definition = try schema_partitions.definitionDigest(scratch, child_json) };
    var after = before;
    after.schema_version = 2;
    const bounds: family.Range = .{ .lower = .minvalue, .upper = .maxvalue };
    var successor = try descriptor.attach(scratch, .{ .relation = after, .bounds = bounds, .inherited = inherited.items });
    successor.parent.schema_version = 2;
    const transition: schema_partitions.Transition = .{ .operation = .attach, .inherited = inherited.items, .before = descriptor, .after = successor, .child_before = before, .child_after = after, .bounds = bounds };
    const installed_json = try transition.schemaAlloc(scratch, 9, child_json);
    var installed = try local.schema_mod.CompiledTableValidator.init(a, installed_json);
    defer installed.deinit(a);
    const membership = installed.schema.sql_partition.?.value.member;
    try membership.require(successor);
    try std.testing.expectEqual(@as(usize, 2), membership.inherited.len);
    var positive = try std.json.parseFromSliceLeaky(std.json.Value, scratch, "{\"at\":1767225600000000000,\"n\":1}", .{});
    _ = try installed.prepareTypedValueWithNulls(scratch, scratch, &positive, &.{}, true);
    var negative = try std.json.parseFromSliceLeaky(std.json.Value, scratch, "{\"at\":1767225600000000000,\"n\":-1}", .{});
    try std.testing.expectError(error.RelationalCheckViolation, installed.prepareTypedValueWithNulls(scratch, scratch, &negative, &.{}, true));
}

test "SQL partition parent routing attach requires generatedness but permits leaf-specific expressions" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const base =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"at":{"type":"datetime"},"n":{"type":"integer"}},"additionalProperties":false}}}
    ;
    const generated_one = base ++
        \\,"generated_columns":[{"column":"n","expression":{"op":"literal","type":"integer","value":1}}]}
    ;
    const generated_two = base ++
        \\,"generated_columns":[{"column":"n","expression":{"op":"literal","type":"integer","value":2}}]}
    ;
    const plain = base ++ "}";
    var inherited: std.ArrayList(family.ConstraintBinding) = .empty;
    try std.testing.expectError(error.SqlPartitionSchemaMismatch, requireExistingConstraints(scratch, generated_one, plain, &inherited));
    try std.testing.expectError(error.SqlPartitionSchemaMismatch, requireExistingConstraints(scratch, plain, generated_two, &inherited));
    try requireExistingConstraints(scratch, generated_one, generated_two, &inherited);
}
