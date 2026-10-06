// Copyright 2026 Antfly, Inc. Licensed under the Elastic License 2.0.
const std = @import("std");
const publication = @import("../fk_generation_publication.zig");
const tables = @import("../table_manager.zig");
const planner_mod = @import("../placement_planner.zig");
const system_catalog = @import("antfly_local_sources").system_catalog_domain;

pub fn run(comptime Store: type) !void {
    const alloc = std.testing.allocator;
    const Case = struct {
        table_role: []const u8,
        node_role: []const u8,
        store_role: []const u8,
        accepted: bool,
        root_incarnation: u128 = 41,
    };
    for ([_]Case{
        .{ .table_role = "", .node_role = "serving", .store_role = "data", .accepted = true },
        .{ .table_role = "", .node_role = "archive", .store_role = "archive", .accepted = true },
        .{ .table_role = "data", .node_role = "data", .store_role = "data", .accepted = true },
        // Canonical policy does not alias data and serving.
        .{ .table_role = "serving", .node_role = "data", .store_role = "data", .accepted = false },
        .{ .table_role = "data", .node_role = "bulk", .store_role = "data", .accepted = false },
        .{ .table_role = "data", .node_role = "data", .store_role = "bulk", .accepted = false },
        .{ .table_role = "", .node_role = "data", .store_role = "data", .accepted = false, .root_incarnation = 0 },
    }) |case| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/initial-role", .{tmp.sub_path});
        defer alloc.free(root);
        var store = try Store.init(alloc, .{ .root_dir = root });
        defer store.deinit();
        const group = @import("antfly_local_sources").common_group_ids.main_metadata_group_id;
        try store.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = "11111111111111111111111111111111".* });
        const logical_schema =
            \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"self_fk","child_columns":["parent_id"],"parent_table":"nodes","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent_id":{"type":"integer"}},"additionalProperties":false}}}}
        ;
        const prepared_json = try store.fkInitialCreatePrepareJson(alloc, group, .{
            .namespace_id = system_catalog.default_namespace_id,
            .logical_name = "nodes",
            .candidate = .{ .table_id = 0, .name = "", .schema_json = logical_schema, .placement_role = case.table_role },
        });
        defer alloc.free(prepared_json);
        var prepared = try std.json.parseFromSlice(publication.InitialCreatePrepare, alloc, prepared_json, .{});
        defer prepared.deinit();
        var child = prepared.value.child;
        child.placement_role = case.table_role;
        const replacement = try std.fmt.allocPrint(alloc, "\"parent_table\":\"{s}\"", .{child.name});
        defer alloc.free(replacement);
        const schema = try std.mem.replaceOwned(u8, alloc, logical_schema, "\"parent_table\":\"nodes\"", replacement);
        defer alloc.free(schema);
        child.schema_json = schema;
        const derived = try publication.deriveInitialTransitions(alloc, child.table_id, child.name, schema);
        defer publication.freeDerivedTransitions(alloc, derived);
        const plan: publication.InitialCreatePlan = .{
            .id = @splat(1),
            .retirement_scope = .hosted_store,
            .catalog_id = prepared.value.catalog_id,
            .expected_catalog_revision = prepared.value.expected_catalog_revision,
            .tablespace_id = prepared.value.tablespace_id,
            .child = child,
            .child_ranges = prepared.value.child_ranges,
            .parents = &.{},
            .self_transitions = &.{derived[0].transition},
            .logical_name = "nodes",
            .namespace_id = system_catalog.default_namespace_id,
        };
        // Seed a coherent admitted publication directly: ordinary CREATE
        // inherits today's tablespace role, while placement authority must
        // also honor any-role plans admitted under a different policy.
        const pending: publication.InitialPublication = .{ .plan = plan, .plan_digest = try plan.digest(alloc), .candidate = try plan.candidateIdentity(alloc), .revision = 1, .phase = .provisioning_child };
        try pending.validateState(alloc);
        const encoded = try std.json.Stringify.valueAlloc(alloc, pending, .{});
        defer alloc.free(encoded);
        var key_buf: [160]u8 = undefined;
        try store.store.put(try publication.initialKey(&key_buf, group, child.table_id), encoded);
        const reservation = (publication.InitialGroupReservation{ .plan_id = plan.id, .plan_digest = pending.plan_digest, .child_table_id = child.table_id, .range_id = plan.child_ranges[0].range_id, .retirement_scope = .hosted_store }).encode();
        try store.store.put(try publication.initialGroupKey(&key_buf, group, plan.child_ranges[0].group_id), &reservation);
        try store.applyStandaloneCommand(group, .{ .register_node = .{ .node_id = 8, .role = case.node_role, .lifecycle = tables.node_lifecycle_active } });
        try store.applyStandaloneCommand(group, .{ .register_store = .{ .store_id = 30, .node_id = 8, .role = case.store_role, .reporter_incarnation = 17, .replica_root_incarnation = case.root_incarnation } });

        var manager = tables.TableManager.init(alloc);
        defer manager.deinit();
        var planner = planner_mod.PlacementPlanner.init(alloc);
        const planned = try planner.planAllIntentsWithPrivate(&manager, &.{8}, &.{}, &.{.{ .node_id = 8, .store_id = 30, .role = case.store_role, .failure_domain = "" }}, &.{}, &.{}, &.{child}, plan.child_ranges);
        defer planner.freeIntents(alloc, planned);
        try std.testing.expectEqual(@as(usize, if (tables.placementRoleCompatible(case.table_role, case.store_role)) 1 else 0), planned.len);
        const intent = if (planned.len != 0) planned[0] else @as(@import("../../raft/reconciler.zig").PlacementIntent, .{
            .record = .{ .group_id = plan.child_ranges[0].group_id, .replica_id = 1, .local_node_id = 8 },
            .store_id = 30,
            .peer_node_ids = &.{8},
        });
        try store.applyStandaloneCommand(group, .{ .upsert_fk_initial_replica_intent = .{
            .proof = .{ .plan_id = plan.id, .plan_digest = try plan.digest(alloc), .child_table_id = child.table_id, .range_id = plan.child_ranges[0].range_id },
            .expected_metadata_version = null,
            .expected_version_fence = 0,
            .expected_target_drain_requested = false,
            .replacement = intent,
        } });
        const actual = try store.listPlacementIntents(alloc, group);
        defer store.freePlacementIntents(alloc, actual);
        if (actual.len != @as(usize, if (case.accepted) 1 else 0)) std.debug.print("initial role admission table='{s}' node='{s}' store='{s}' root={} planned={} actual={}\n", .{ case.table_role, case.node_role, case.store_role, case.root_incarnation, planned.len, actual.len });
        try std.testing.expectEqual(@as(usize, if (case.accepted) 1 else 0), actual.len);
        if (case.accepted) try std.testing.expect(actual[0].record.initial_fk_root_generation != 0);
    }
}
