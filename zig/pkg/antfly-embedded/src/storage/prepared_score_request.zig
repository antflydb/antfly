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

//! Owned immutable graph request. Inputs are copied at construction; callers
//! keep the request alive during delivery and never mutate its owned plan.
const std = @import("std");
const Plan = @import("prepared_score_plan.zig").PreparedScorePlan;

pub const PreparedScoreRequest = struct {
    // The pointee is immutable outside this owner. Moving the handle is fine;
    // copying it does not transfer ownership and must not produce two deinits.
    handle: *const State,
    const State = struct { allocator: std.mem.Allocator, plan: Plan };

    pub fn init(allocator: std.mem.Allocator, prefixes: []const ?[]const u8, nodes: []const []const u8) !PreparedScoreRequest {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.* = .{ .allocator = allocator, .plan = .{} };
        errdefer state.plan.deinit();
        if (!try state.plan.prepare(allocator, prefixes, nodes)) return error.GraphMetricQueryBudgetExceeded;
        return .{ .handle = state };
    }
    /// Clone while the source scope is exclusively leased. Independent lifetime
    /// ownership preserves its canonical identity and any validated score cache.
    pub fn fromPlan(allocator: std.mem.Allocator, source: *const Plan) !PreparedScoreRequest {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.* = .{ .allocator = allocator, .plan = try source.clone(allocator) };
        return .{ .handle = state };
    }
    pub fn plan(self: *const PreparedScoreRequest) *const Plan {
        return &self.handle.plan;
    }
    pub fn deinit(self: *PreparedScoreRequest) void {
        const state = @constCast(self.handle);
        const allocator = state.allocator;
        state.plan.deinit();
        allocator.destroy(state);
        self.* = undefined;
    }
};

test "prepared score request owns immutable inputs and unique identity" {
    var node: [1]u8 = .{'a'};
    var prefix: [2]u8 = .{ 'p', '/' };
    var request = try PreparedScoreRequest.init(std.testing.allocator, &.{&prefix}, &.{&node});
    defer request.deinit();
    node[0] = 'b';
    prefix[0] = 'q';
    try std.testing.expectEqualStrings("a", request.plan().nodes[0]);
    try std.testing.expectEqualStrings("p/", request.plan().prefixes[0].?);
    var next = try PreparedScoreRequest.init(std.testing.allocator, &.{"p/"}, &.{"a"});
    defer next.deinit();
    try std.testing.expect(request.plan().identity != next.plan().identity);
}

fn allocationFailureCase(a: std.mem.Allocator) !void {
    var request = try PreparedScoreRequest.init(a, &.{ "p/", null, "p/" }, &.{ "a", "b", "a" });
    defer request.deinit();
    try std.testing.expect(request.plan().valid);
}

test "prepared score request allocation failures release ownership" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationFailureCase, .{});
}

fn cloneFailureCase(a: std.mem.Allocator, source: *const Plan) !void {
    var request = try PreparedScoreRequest.fromPlan(a, source);
    defer request.deinit();
    try std.testing.expectEqual(source.identity, request.plan().identity);
}

test "prepared score request cloning allocation failures retain source ownership" {
    var source = Plan{};
    defer source.deinit();
    try std.testing.expect(try source.prepare(std.testing.allocator, &.{ "", null, "p/" }, &.{ "", "a", "a" }));
    try std.testing.checkAllAllocationFailures(std.testing.allocator, cloneFailureCase, .{&source});
    try std.testing.expect(source.matches(&.{ "", null, "p/" }, &.{ "", "a", "a" }));
}
