// Copyright 2026 Antfly, Inc.
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

//! Determinism-kernel helpers shared by runners and scenario adapters.

const std = @import("std");
const transition = @import("transition.zig");

pub fn enumerateCanonical(
    comptime Scenario: type,
    world: *Scenario.World,
    allocator: std.mem.Allocator,
) !transition.List {
    var result = transition.List{};
    errdefer result.deinit(allocator);
    try Scenario.enumerate(world, &result, allocator);
    try result.canonicalize();
    return result;
}

/// Candidate enumeration is required to be side-effect free. Re-enumerating
/// immediately before execution both checks that contract and catches an
/// adapter that selected an operation which is no longer enabled.
pub fn verifyStillEnabled(
    comptime Scenario: type,
    world: *Scenario.World,
    allocator: std.mem.Allocator,
    original: []const transition.Transition,
    selected: transition.Transition,
) !void {
    var current = try enumerateCanonical(Scenario, world, allocator);
    defer current.deinit(allocator);
    if (current.items.items.len != original.len) return error.EnabledSetChangedDuringSelection;
    var found = false;
    for (original, current.items.items) |before, after| {
        if (!transition.Transition.eql(before, after)) return error.EnabledSetChangedDuringSelection;
        if (after.id == selected.id) {
            if (!transition.Transition.eql(after, selected)) return error.SelectedTransitionChanged;
            found = true;
        }
    }
    if (!found) return error.SelectedTransitionNoLongerEnabled;
}

test "scheduler detects side effects during enumeration" {
    const Scenario = struct {
        const World = struct { enumerations: u64 = 0 };
        fn enumerate(world: *World, list: *transition.List, allocator: std.mem.Allocator) !void {
            world.enumerations += 1;
            try list.append(allocator, .{
                .id = world.enumerations,
                .name = "unstable",
                .kind = .scheduler,
            });
        }
    };
    var world = Scenario.World{};
    var original = try enumerateCanonical(Scenario, &world, std.testing.allocator);
    defer original.deinit(std.testing.allocator);
    try std.testing.expectError(
        error.EnabledSetChangedDuringSelection,
        verifyStillEnabled(Scenario, &world, std.testing.allocator, original.items.items, original.items.items[0]),
    );
}
