// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const service = @import("metadata/service.zig");
const catalog_projection_reader = @import("metadata/catalog_projection_reader.zig");
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
const relational_topology_admission = @import("metadata/relational_topology_admission.zig");
const transition_actions = @import("metadata/transition_actions.zig");
const transition_controller = @import("metadata/transition_controller.zig");
const transition_driver = @import("metadata/transition_driver.zig");
const replication_backfill = @import("metadata/replication_backfill.zig");

comptime {
    _ = service;
    _ = catalog_projection_reader;
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
    _ = relational_topology_admission;
    _ = transition_actions;
    _ = transition_controller;
    _ = transition_driver;
    _ = @import("metadata/online_merge.zig");
    _ = @import("metadata/online_merge_driver.zig");
    _ = replication_backfill;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};

// Keep the action contract in this lane even when compile filters prune the
// leaf module's otherwise unreferenced tests.
test "transition actions module compiles" {
    _ = transition_actions.TransitionAction;
    _ = transition_actions.TransitionDecision;
}
