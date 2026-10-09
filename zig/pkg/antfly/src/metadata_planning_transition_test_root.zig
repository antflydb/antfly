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

const placement_planner = @import("metadata/placement_planner.zig");
const control_loop = @import("metadata/control_loop.zig");
const table_manager = @import("metadata/table_manager.zig");
const table_workflow = @import("metadata/table_workflow.zig");
const transition_state = @import("metadata/transition_state.zig");
const relational_topology_admission = @import("metadata/relational_topology_admission.zig");
const transition_actions = @import("metadata/transition_actions.zig");
const transition_controller = @import("metadata/transition_controller.zig");
const transition_driver = @import("metadata/transition_driver.zig");

test {
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
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};

// Force the action contract even when the discovery anchor is filtered out.
comptime {
    _ = transition_actions.TransitionAction;
    _ = transition_actions.TransitionDecision;
}
