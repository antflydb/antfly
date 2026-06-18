// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const openapi = @import("antfly_internal_openapi");

pub const base = "/internal/v1";
pub const ha = base ++ "/ha";
pub const ha_replication = ha ++ "/replication";
pub const ha_replication_identify = ha_replication ++ "/identify";
pub const ha_replication_slots = ha_replication ++ "/slots";
pub const ha_replication_start = ha_replication ++ "/start";
pub const ha_replication_status = ha_replication ++ "/status";

test "internal routes define HA runtime replication paths" {
    try std.testing.expectEqualStrings("/internal/v1/ha/replication/identify", ha_replication_identify);
    try std.testing.expectEqualStrings("/internal/v1/ha/replication/slots", ha_replication_slots);
    try std.testing.expectEqualStrings("/internal/v1/ha/replication/start", ha_replication_start);
    try std.testing.expectEqualStrings("/internal/v1/ha/replication/status", ha_replication_status);
}

test "internal routes match generated OpenAPI HA replication operations" {
    for (expected_ha_replication_routes) |route| {
        try expectGeneratedRoute(route.operation_id, route.method, route.full_path);
    }
    try expectEveryGeneratedHAReplicationRouteCovered();
}

const ExpectedRoute = struct {
    operation_id: []const u8,
    method: []const u8,
    full_path: []const u8,
};

const expected_ha_replication_routes = [_]ExpectedRoute{
    .{ .operation_id = "identifyHAReplicationSystem", .method = "GET", .full_path = ha_replication_identify },
    .{ .operation_id = "createHAReplicationStreamingSlot", .method = "POST", .full_path = ha_replication_slots },
    .{ .operation_id = "startHAReplication", .method = "POST", .full_path = ha_replication_start },
    .{ .operation_id = "updateHAStandbyStatus", .method = "POST", .full_path = ha_replication_status },
};

fn expectGeneratedRoute(operation_id: []const u8, method: []const u8, full_path: []const u8) !void {
    try std.testing.expect(std.mem.startsWith(u8, full_path, base));
    const spec_path = full_path[base.len..];

    for (openapi.server.routes) |route| {
        if (!std.mem.eql(u8, route.operation_id, operation_id)) continue;
        try std.testing.expectEqualStrings(method, route.method);
        try std.testing.expectEqualStrings(spec_path, route.path);
        return;
    }

    std.debug.print("missing generated internal OpenAPI route for operation '{s}'\n", .{operation_id});
    return error.TestExpectedGeneratedRoute;
}

fn expectEveryGeneratedHAReplicationRouteCovered() !void {
    for (openapi.server.routes) |generated| {
        if (!std.mem.startsWith(u8, generated.path, "/ha/replication/")) continue;
        if (expectedHAReplicationRoute(generated) != null) continue;

        std.debug.print(
            "generated internal OpenAPI HA replication route {s} {s} ({s}) is not covered by zig/pkg/antfly/src/internal/routes.zig\n",
            .{ generated.method, generated.path, generated.operation_id },
        );
        return error.TestExpectedGeneratedRouteCovered;
    }
}

fn expectedHAReplicationRoute(generated: openapi.server.Route) ?ExpectedRoute {
    for (expected_ha_replication_routes) |expected| {
        if (!std.mem.eql(u8, generated.operation_id, expected.operation_id)) continue;
        if (!std.mem.eql(u8, generated.method, expected.method)) continue;
        if (!std.mem.startsWith(u8, expected.full_path, base)) continue;
        if (!std.mem.eql(u8, generated.path, expected.full_path[base.len..])) continue;
        return expected;
    }
    return null;
}
