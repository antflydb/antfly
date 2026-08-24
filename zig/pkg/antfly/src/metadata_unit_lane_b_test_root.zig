// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const service = @import("metadata/service.zig");
const admin_read_operations = @import("metadata/admin_read_operations.zig");
const admin_mutation_operations = @import("metadata/admin_mutation_operations.zig");
const extension_operations = @import("metadata/extension_operations.zig");
const node_operations = @import("metadata/node_operations.zig");
const table_operations = @import("metadata/table_operations.zig");
const http_client = @import("metadata/http_client.zig");
const http_routes = @import("metadata/http_routes.zig");
const http_server = @import("metadata/http_server.zig");
const api = @import("metadata/api.zig");
const admin = @import("metadata/admin.zig");
const placement_planner = @import("metadata/placement_planner.zig");
const control_loop = @import("metadata/control_loop.zig");
const table_manager = @import("metadata/table_manager.zig");
const table_workflow = @import("metadata/table_workflow.zig");
const transition_state = @import("metadata/transition_state.zig");
const transition_actions = @import("metadata/transition_actions.zig");
const transition_controller = @import("metadata/transition_controller.zig");
const transition_driver = @import("metadata/transition_driver.zig");
const replication_backfill = @import("metadata/replication_backfill.zig");

test {
    _ = service;
    _ = admin_read_operations;
    _ = admin_mutation_operations;
    _ = extension_operations;
    _ = node_operations;
    _ = table_operations;
    _ = http_client;
    _ = http_routes;
    _ = http_server;
    _ = api;
    _ = admin;
    _ = placement_planner;
    _ = control_loop;
    _ = table_manager;
    _ = table_workflow;
    _ = transition_state;
    _ = transition_actions;
    _ = transition_controller;
    _ = transition_driver;
    _ = replication_backfill;
}
