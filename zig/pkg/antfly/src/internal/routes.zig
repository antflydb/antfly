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

const std = @import("std");
const openapi = @import("antfly_internal_openapi");

pub const base = "/internal/v1";
pub const remote_attempt_status = base ++ "/attempt/status";
pub const remote_attempt_fence = base ++ "/attempt/fence";
pub const remote_attempt_cancel = base ++ "/attempt/cancel";
pub const remote_attempt_capabilities = base ++ "/attempt/capabilities";

pub fn isRemoteAttemptControl(path: []const u8) bool {
    return std.mem.eql(u8, path, remote_attempt_status) or std.mem.eql(u8, path, remote_attempt_fence) or std.mem.eql(u8, path, remote_attempt_cancel) or std.mem.eql(u8, path, remote_attempt_capabilities);
}

/// Only the group-local buffered query has a physical owned-read adapter.
/// Match the route shape exactly; preflight, streams and aliases have no grant.
pub fn isOwnedGroupQuery(path: []const u8) bool {
    return groupTableAction(path, "query");
}
pub fn isRetainedRead(path: []const u8) bool {
    return groupTableAction(path, "retained-read");
}
pub const ReadScope = struct { root: u128, reporter: u64 };
const ReadRoute = struct { retained: bool, scope: ?ReadScope };
fn readRoute(path: []const u8) ?ReadRoute {
    if (std.mem.indexOfAny(u8, path, "?#\r\n") != null) return null;
    var parts = std.mem.splitScalar(u8, path, '/');
    for ([_][]const u8{ "", "internal", "v1", "groups" }) |part| {
        if (!std.mem.eql(u8, parts.next() orelse return null, part)) return null;
    }
    const group = std.fmt.parseUnsigned(u64, parts.next() orelse return null, 10) catch return null;
    if (group == 0 or !std.mem.eql(u8, parts.next() orelse return null, "tables")) return null;
    if ((parts.next() orelse return null).len == 0) return null;
    const action = parts.next() orelse return null;
    if (std.mem.eql(u8, action, "query") or std.mem.eql(u8, action, "retained-read")) {
        if (parts.next() != null) return null;
        return .{ .retained = std.mem.eql(u8, action, "retained-read"), .scope = null };
    }
    if (!std.mem.eql(u8, action, "owned-query") and !std.mem.eql(u8, action, "owned-retained-read")) return null;
    const root = std.fmt.parseUnsigned(u128, parts.next() orelse return null, 10) catch return null;
    const reporter = std.fmt.parseUnsigned(u64, parts.next() orelse return null, 10) catch return null;
    if (root == 0 or reporter == 0 or parts.next() != null) return null;
    return .{ .retained = std.mem.eql(u8, action, "owned-retained-read"), .scope = .{ .root = root, .reporter = reporter } };
}
fn groupTableAction(path: []const u8, action: []const u8) bool {
    const route = readRoute(path) orelse return false;
    return route.retained == std.mem.eql(u8, action, "retained-read");
}
/// Signed data always uses a distinct endpoint, never a legacy query route.
/// The URI digest authenticates the exact durable root and process reporter.
pub fn ownedReadPath(allocator: std.mem.Allocator, path: []const u8, root: u128, reporter: u64) ![]u8 {
    if (root == 0 or reporter == 0 or !std.mem.startsWith(u8, path, "/internal/v1/") or std.mem.indexOfAny(u8, path, "#\r\n") != null) return error.InvalidRemoteAttemptTarget;
    const route = readRoute(path) orelse return error.UnsupportedRemoteAttemptProtocol;
    if (route.scope) |_| {
        try validateOwnedReadScope(path, root, reporter);
        return allocator.dupe(u8, path);
    }
    const leaf = std.mem.lastIndexOfScalar(u8, path, '/').? + 1;
    return std.fmt.allocPrint(allocator, "{s}{s}/{d}/{d}", .{ path[0..leaf], if (route.retained) "owned-retained-read" else "owned-query", root, reporter });
}
pub fn validateOwnedReadScope(path: []const u8, root: u128, reporter: u64) !void {
    const route = readRoute(path) orelse return error.UnsupportedRemoteAttemptProtocol;
    const scope = route.scope orelse return error.UnsupportedRemoteAttemptProtocol;
    if (scope.root != root or scope.reporter != reporter) return error.RemoteWorkerIdentityChanged;
}

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
pub const hot_standby_replication = standby_replication;
pub const hot_standby_replication_identify = standby_replication_identify;
pub const hot_standby_replication_slots = standby_replication_slots;
pub const hot_standby_replication_start = standby_replication_start;
pub const hot_standby_replication_status = standby_replication_status;

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
    try std.testing.expectEqualStrings(standby_replication_identify, hot_standby_replication_identify);
}

test "internal routes match generated OpenAPI standby replication operations" {
    for (expected_hot_standby_replication_routes) |route| {
        try expectGeneratedRoute(route.operation_id, route.method, route.full_path);
    }
    try expectEveryGeneratedHotStandbyReplicationRouteCovered();
}

const ExpectedRoute = struct {
    operation_id: []const u8,
    method: []const u8,
    full_path: []const u8,
};

const expected_hot_standby_replication_routes = [_]ExpectedRoute{
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

fn expectEveryGeneratedHotStandbyReplicationRouteCovered() !void {
    for (openapi.server.routes) |generated| {
        if (!std.mem.startsWith(u8, generated.path, "/standby/replication/")) continue;
        if (expectedHotStandbyReplicationRoute(generated) != null) continue;

        std.debug.print(
            "generated internal OpenAPI HA replication route {s} {s} ({s}) is not covered by zig/pkg/antfly/src/internal/routes.zig\n",
            .{ generated.method, generated.path, generated.operation_id },
        );
        return error.TestExpectedGeneratedRouteCovered;
    }
}

fn expectedHotStandbyReplicationRoute(generated: openapi.server.Route) ?ExpectedRoute {
    for (expected_hot_standby_replication_routes) |expected| {
        if (!std.mem.eql(u8, generated.operation_id, expected.operation_id)) continue;
        if (!std.mem.eql(u8, generated.method, expected.method)) continue;
        if (!std.mem.startsWith(u8, expected.full_path, base)) continue;
        if (!std.mem.eql(u8, generated.path, expected.full_path[base.len..])) continue;
        return expected;
    }
    return null;
}

test "remote attempt wire owned paths bind root and process and cannot match legacy read routes" {
    const a = std.testing.allocator;
    const legacy = "/internal/v1/groups/7/tables/docs/query";
    const path = try ownedReadPath(a, legacy, 11, 17);
    defer a.free(path);
    try std.testing.expectEqualStrings("/internal/v1/groups/7/tables/docs/owned-query/11/17", path);
    try std.testing.expect(isOwnedGroupQuery(path));
    try std.testing.expect(!isRetainedRead(path));
    try validateOwnedReadScope(path, 11, 17);
    try std.testing.expectError(error.UnsupportedRemoteAttemptProtocol, validateOwnedReadScope(legacy, 11, 17));
    try std.testing.expectError(error.RemoteWorkerIdentityChanged, validateOwnedReadScope(path, 11, 18));
    try std.testing.expectError(error.RemoteWorkerIdentityChanged, ownedReadPath(a, path, 22, 17));
    const repeated = try ownedReadPath(a, path, 11, 17);
    defer a.free(repeated);
    try std.testing.expectEqualStrings(path, repeated);
    const retained = try ownedReadPath(a, "/internal/v1/groups/7/tables/docs/retained-read", std.math.maxInt(u128), std.math.maxInt(u64));
    defer a.free(retained);
    try std.testing.expect(isRetainedRead(retained));
    try validateOwnedReadScope(retained, std.math.maxInt(u128), std.math.maxInt(u64));
    for ([_][]const u8{ "/internal/v1/groups/7/tables/docs/owned-query/11/0", "/internal/v1/groups/7/tables/docs/owned-query/0/17", "/internal/v1/groups/7/tables/docs/owned-query/11/18446744073709551616", "/internal/v1/groups/7/tables/docs/owned-query/11/17/extra", "/internal/v1/groups/7/tables/docs/owned-query/11/17?legacy=1" }) |invalid| {
        try std.testing.expect(!isOwnedGroupQuery(invalid));
    }
    var empty: [0]u8 = .{};
    var oom = std.heap.FixedBufferAllocator.init(&empty);
    try std.testing.expectError(error.OutOfMemory, ownedReadPath(oom.allocator(), legacy, 11, 17));
}
