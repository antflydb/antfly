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
