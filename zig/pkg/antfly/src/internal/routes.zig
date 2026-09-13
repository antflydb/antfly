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
pub const standby = base ++ "/standby";
pub const standby_replication = standby ++ "/replication";
pub const standby_replication_identify = standby_replication ++ "/identify";
pub const standby_replication_slots = standby_replication ++ "/slots";
pub const standby_replication_start = standby_replication ++ "/start";
pub const standby_replication_status = standby_replication ++ "/status";

/// The pre-0.3 spelling of the replication routes. Servers keep answering on
/// it for one minor release so a rolling upgrade can mix versions; remove
/// after 0.4.
pub const legacy_standby = base ++ "/ha";
pub const legacy_standby_replication = legacy_standby ++ "/replication";
pub const legacy_standby_replication_identify = legacy_standby_replication ++ "/identify";
pub const legacy_standby_replication_slots = legacy_standby_replication ++ "/slots";
pub const legacy_standby_replication_start = legacy_standby_replication ++ "/start";
pub const legacy_standby_replication_status = legacy_standby_replication ++ "/status";

// Deprecated aliases for the canonical constants above; remove after 0.4.
pub const ha = standby;
pub const ha_replication = standby_replication;
pub const ha_replication_identify = standby_replication_identify;
pub const ha_replication_slots = standby_replication_slots;
pub const ha_replication_start = standby_replication_start;
pub const ha_replication_status = standby_replication_status;

const alias_pairs = [_][2][]const u8{
    .{ legacy_standby_replication_identify, standby_replication_identify },
    .{ legacy_standby_replication_slots, standby_replication_slots },
    .{ legacy_standby_replication_start, standby_replication_start },
    .{ legacy_standby_replication_status, standby_replication_status },
};

/// Maps a legacy `/internal/v1/ha/replication/...` request path onto its
/// canonical constant. Every replication route is fixed, so this needs no
/// allocation; unrelated paths are returned unchanged.
pub fn canonicalReplicationPath(path: []const u8) []const u8 {
    for (alias_pairs) |pair| {
        if (std.mem.eql(u8, path, pair[0])) return pair[1];
    }
    return path;
}

/// Inverse of `canonicalReplicationPath`, used by clients that must fall back
/// to a pre-0.3 primary.
pub fn legacyReplicationPath(path: []const u8) []const u8 {
    for (alias_pairs) |pair| {
        if (std.mem.eql(u8, path, pair[1])) return pair[0];
    }
    return path;
}

test "internal routes define standby replication paths and their legacy aliases" {
    try std.testing.expectEqualStrings("/internal/v1/standby/replication/identify", standby_replication_identify);
    try std.testing.expectEqualStrings("/internal/v1/standby/replication/slots", standby_replication_slots);
    try std.testing.expectEqualStrings("/internal/v1/standby/replication/start", standby_replication_start);
    try std.testing.expectEqualStrings("/internal/v1/standby/replication/status", standby_replication_status);
    try std.testing.expectEqualStrings("/internal/v1/ha/replication/identify", legacy_standby_replication_identify);
    try std.testing.expectEqualStrings(standby_replication_status, canonicalReplicationPath(legacy_standby_replication_status));
    try std.testing.expectEqualStrings(standby_replication_status, canonicalReplicationPath(standby_replication_status));
    try std.testing.expectEqualStrings(legacy_standby_replication_start, legacyReplicationPath(standby_replication_start));
    try std.testing.expectEqualStrings("/internal/v1/other", canonicalReplicationPath("/internal/v1/other"));
    try std.testing.expectEqualStrings(standby_replication_identify, ha_replication_identify);
}

test "internal routes match generated OpenAPI standby replication operations" {
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
    .{ .operation_id = "identifyHAReplicationSystem", .method = "GET", .full_path = standby_replication_identify },
    .{ .operation_id = "createHAReplicationStreamingSlot", .method = "POST", .full_path = standby_replication_slots },
    .{ .operation_id = "startHAReplication", .method = "POST", .full_path = standby_replication_start },
    .{ .operation_id = "updateHAStandbyStatus", .method = "POST", .full_path = standby_replication_status },
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
        if (!std.mem.startsWith(u8, generated.path, "/standby/replication/")) continue;
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
