// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const placement_planner = @import("metadata/placement_planner.zig");
const control_loop = @import("metadata/control_loop.zig");
const table_manager = @import("metadata/table_manager.zig");
const table_workflow = @import("metadata/table_workflow.zig");
const transition_state = @import("metadata/transition_state.zig");
const transition_actions = @import("metadata/transition_actions.zig");
const transition_controller = @import("metadata/transition_controller.zig");
const transition_driver = @import("metadata/transition_driver.zig");

test {
    _ = placement_planner;
    _ = control_loop;
    _ = table_manager;
    _ = table_workflow;
    _ = transition_state;
    _ = transition_actions;
    _ = transition_controller;
    _ = transition_driver;
}
