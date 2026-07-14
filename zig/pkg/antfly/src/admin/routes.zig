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
const Allocator = std.mem.Allocator;
const openapi = @import("antfly_admin_openapi");

pub const base = "/admin/v1";
pub const maintenance = base ++ "/maintenance";
pub const maintenance_check = maintenance ++ "/check";
pub const maintenance_compact = maintenance ++ "/compact";
pub const maintenance_vacuum = maintenance ++ "/vacuum";
pub const maintenance_jobs = maintenance ++ "/jobs";
pub const maintenance_jobs_prefix = maintenance_jobs ++ "/";
pub const ha = base ++ "/ha";
pub const ha_primary_status = ha ++ "/primary/status";
pub const ha_standby_status = ha ++ "/standby/status";
pub const ha_commit_check = ha ++ "/commit/check";
pub const ha_commit_append = ha ++ "/commit/append";
pub const ha_read_check = ha ++ "/read/check";
pub const ha_write_check = ha ++ "/write/check";
pub const ha_owner_job_check = ha ++ "/owner-jobs/check";
pub const ha_replication_slots = ha ++ "/replication-slots";
pub const ha_replication_slot_prefix = ha_replication_slots ++ "/";
pub const ha_replication_slot_pause_suffix = "/pause";
pub const ha_replication_slot_resume_suffix = "/resume";
pub const ha_base_backups = ha ++ "/base-backups";
pub const ha_base_backups_finish = ha_base_backups ++ "/finish";
pub const ha_base_backups_capture = ha_base_backups ++ "/capture";
pub const ha_base_backups_activate = ha_base_backups ++ "/activate";
pub const ha_seed_lifecycle_receipts = ha ++ "/seed-lifecycle/receipts";
pub const ha_standby_bootstrap = ha ++ "/standby/bootstrap";
pub const ha_fence = ha ++ "/fence";
pub const ha_fence_current = ha_fence ++ "/current";
pub const ha_promotion = ha ++ "/promotion";
pub const ha_promotion_assess = ha_promotion ++ "/assess";
pub const ha_promotion_current_fence = ha_promotion ++ "/current-fence";
pub const ha_rejoin_assess = ha ++ "/rejoin/assess";
pub const ha_rejoin_rewind = ha ++ "/rejoin/rewind";
pub const ha_rejoin_reseed = ha ++ "/rejoin/reseed";

pub fn replicationSlotPathAlloc(alloc: Allocator, slot_name: []const u8) ![]u8 {
    const escaped = try percentEncodePathSegmentAlloc(alloc, slot_name);
    defer alloc.free(escaped);
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ ha_replication_slot_prefix, escaped });
}

pub fn replicationSlotPausePathAlloc(alloc: Allocator, slot_name: []const u8) ![]u8 {
    const escaped = try percentEncodePathSegmentAlloc(alloc, slot_name);
    defer alloc.free(escaped);
    return try std.fmt.allocPrint(alloc, "{s}{s}{s}", .{
        ha_replication_slot_prefix,
        escaped,
        ha_replication_slot_pause_suffix,
    });
}

pub fn replicationSlotResumePathAlloc(alloc: Allocator, slot_name: []const u8) ![]u8 {
    const escaped = try percentEncodePathSegmentAlloc(alloc, slot_name);
    defer alloc.free(escaped);
    return try std.fmt.allocPrint(alloc, "{s}{s}{s}", .{
        ha_replication_slot_prefix,
        escaped,
        ha_replication_slot_resume_suffix,
    });
}

pub fn replicationSlotNameFromPath(path: []const u8, suffix: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, path, ha_replication_slot_prefix)) return null;
    if (!std.mem.endsWith(u8, path, suffix)) return null;

    const name_start = ha_replication_slot_prefix.len;
    const name_end = path.len - suffix.len;
    if (name_end <= name_start) return null;

    const name = path[name_start..name_end];
    if (std.mem.indexOfScalar(u8, name, '/') != null) return null;
    return name;
}

pub fn replicationSlotNameFromPathAlloc(alloc: Allocator, path: []const u8, suffix: []const u8) !?[]u8 {
    const encoded = replicationSlotNameFromPath(path, suffix) orelse return null;
    return try percentDecodePathSegmentAlloc(alloc, encoded);
}

pub fn percentEncodePathSegmentAlloc(alloc: Allocator, raw: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (raw) |byte| {
        if (isPathSegmentUnreserved(byte)) {
            try out.append(alloc, byte);
        } else {
            var buf: [3]u8 = undefined;
            const encoded = try std.fmt.bufPrint(&buf, "%{X:0>2}", .{byte});
            try out.appendSlice(alloc, encoded);
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn percentDecodePathSegmentAlloc(alloc: Allocator, encoded: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var idx: usize = 0;
    while (idx < encoded.len) {
        const byte = encoded[idx];
        if (byte != '%') {
            try out.append(alloc, byte);
            idx += 1;
            continue;
        }
        if (idx + 2 >= encoded.len) return error.InvalidPercentEncoding;
        const hi = hexValue(encoded[idx + 1]) orelse return error.InvalidPercentEncoding;
        const lo = hexValue(encoded[idx + 2]) orelse return error.InvalidPercentEncoding;
        try out.append(alloc, (hi << 4) | lo);
        idx += 3;
    }

    return try out.toOwnedSlice(alloc);
}

fn hexValue(byte: u8) ?u8 {
    return switch (byte) {
        '0'...'9' => byte - '0',
        'a'...'f' => byte - 'a' + 10,
        'A'...'F' => byte - 'A' + 10,
        else => null,
    };
}

fn isPathSegmentUnreserved(byte: u8) bool {
    return (byte >= 'A' and byte <= 'Z') or
        (byte >= 'a' and byte <= 'z') or
        (byte >= '0' and byte <= '9') or
        byte == '-' or byte == '.' or byte == '_' or byte == '~';
}

test "admin routes define HA control-plane paths" {
    try std.testing.expectEqualStrings("/admin/v1/ha/primary/status", ha_primary_status);
    try std.testing.expectEqualStrings("/admin/v1/ha/standby/status", ha_standby_status);
    try std.testing.expectEqualStrings("/admin/v1/ha/commit/check", ha_commit_check);
    try std.testing.expectEqualStrings("/admin/v1/ha/commit/append", ha_commit_append);
    try std.testing.expectEqualStrings("/admin/v1/ha/read/check", ha_read_check);
    try std.testing.expectEqualStrings("/admin/v1/ha/write/check", ha_write_check);
    try std.testing.expectEqualStrings("/admin/v1/ha/owner-jobs/check", ha_owner_job_check);
    try std.testing.expectEqualStrings("/admin/v1/ha/replication-slots", ha_replication_slots);
    try std.testing.expectEqualStrings("/admin/v1/ha/base-backups", ha_base_backups);
    try std.testing.expectEqualStrings("/admin/v1/ha/base-backups/finish", ha_base_backups_finish);
    try std.testing.expectEqualStrings("/admin/v1/ha/base-backups/activate", ha_base_backups_activate);
    try std.testing.expectEqualStrings("/admin/v1/ha/seed-lifecycle/receipts", ha_seed_lifecycle_receipts);
    try std.testing.expectEqualStrings("/admin/v1/ha/standby/bootstrap", ha_standby_bootstrap);
    try std.testing.expectEqualStrings("/admin/v1/ha/fence", ha_fence);
    try std.testing.expectEqualStrings("/admin/v1/ha/fence/current", ha_fence_current);
    try std.testing.expectEqualStrings("/admin/v1/ha/promotion", ha_promotion);
    try std.testing.expectEqualStrings("/admin/v1/ha/promotion/assess", ha_promotion_assess);
    try std.testing.expectEqualStrings("/admin/v1/ha/promotion/current-fence", ha_promotion_current_fence);
    try std.testing.expectEqualStrings("/admin/v1/ha/rejoin/assess", ha_rejoin_assess);
    try std.testing.expectEqualStrings("/admin/v1/ha/rejoin/rewind", ha_rejoin_rewind);
    try std.testing.expectEqualStrings("/admin/v1/ha/rejoin/reseed", ha_rejoin_reseed);
}

test "admin routes define storage-neutral maintenance paths" {
    try std.testing.expectEqualStrings("/admin/v1/maintenance/check", maintenance_check);
    try std.testing.expectEqualStrings("/admin/v1/maintenance/compact", maintenance_compact);
    try std.testing.expectEqualStrings("/admin/v1/maintenance/vacuum", maintenance_vacuum);
    try std.testing.expectEqualStrings("/admin/v1/maintenance/jobs/", maintenance_jobs_prefix);
}

test "admin routes match generated OpenAPI HA operations" {
    for (expected_ha_routes) |route| {
        try expectGeneratedRoute(route.operation_id, route.method, route.full_path);
    }
    try expectEveryGeneratedHARouteCovered();
}

test "admin routes own HA admin path literals consumed by Zig runtime code" {
    try expectNoHardCodedHAAdminPath("../cmd/ha.zig", @embedFile("../cmd/ha.zig"));
    try expectNoHardCodedHAAdminPath("../storage/ha/admin_exec.zig", @embedFile("../storage/ha/admin_exec.zig"));
    try expectNoHardCodedHAAdminPath("../storage/ha/http_admin.zig", @embedFile("../storage/ha/http_admin.zig"));
    try expectNoHardCodedHAAdminPath("../storage/ha/http_client.zig", @embedFile("../storage/ha/http_client.zig"));
    try expectNoHardCodedHAAdminPath("../storage/ha/operator.zig", @embedFile("../storage/ha/operator.zig"));
    try expectNoHardCodedHAAdminPath("../standalone/runtime.zig", @embedFile("../standalone/runtime.zig"));
}

test "admin routes build and match replication slot lifecycle paths" {
    const alloc = std.testing.allocator;

    const slot_path = try replicationSlotPathAlloc(alloc, "standby-a");
    defer alloc.free(slot_path);
    try std.testing.expectEqualStrings("/admin/v1/ha/replication-slots/standby-a", slot_path);
    try std.testing.expectEqualStrings(
        "standby-a",
        replicationSlotNameFromPath(slot_path, "").?,
    );

    const pause_path = try replicationSlotPausePathAlloc(alloc, "standby-a");
    defer alloc.free(pause_path);
    try std.testing.expectEqualStrings("/admin/v1/ha/replication-slots/standby-a/pause", pause_path);
    try std.testing.expectEqualStrings(
        "standby-a",
        replicationSlotNameFromPath(pause_path, ha_replication_slot_pause_suffix).?,
    );

    const resume_path = try replicationSlotResumePathAlloc(alloc, "standby-a");
    defer alloc.free(resume_path);
    try std.testing.expectEqualStrings("/admin/v1/ha/replication-slots/standby-a/resume", resume_path);
    try std.testing.expectEqualStrings(
        "standby-a",
        replicationSlotNameFromPath(resume_path, ha_replication_slot_resume_suffix).?,
    );

    try std.testing.expect(replicationSlotNameFromPath("/admin/v1/ha/replication-slots/standby-a/extra", "") == null);
    try std.testing.expect(replicationSlotNameFromPath("/admin/v1/ha/replication-slots/standby-a", ha_replication_slot_pause_suffix) == null);
}

test "admin routes encode and decode replication slot path segments" {
    const alloc = std.testing.allocator;

    const slot_name = "standby/a b%";
    const pause_path = try replicationSlotPausePathAlloc(alloc, slot_name);
    defer alloc.free(pause_path);
    try std.testing.expectEqualStrings("/admin/v1/ha/replication-slots/standby%2Fa%20b%25/pause", pause_path);
    try std.testing.expectEqualStrings("standby%2Fa%20b%25", replicationSlotNameFromPath(pause_path, ha_replication_slot_pause_suffix).?);

    const decoded = (try replicationSlotNameFromPathAlloc(alloc, pause_path, ha_replication_slot_pause_suffix)).?;
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings(slot_name, decoded);

    try std.testing.expectError(
        error.InvalidPercentEncoding,
        replicationSlotNameFromPathAlloc(alloc, "/admin/v1/ha/replication-slots/standby%2", ""),
    );
    try std.testing.expectError(
        error.InvalidPercentEncoding,
        replicationSlotNameFromPathAlloc(alloc, "/admin/v1/ha/replication-slots/standby%XX", ""),
    );
}

const ExpectedRoute = struct {
    operation_id: []const u8,
    method: []const u8,
    full_path: []const u8,
};

const expected_ha_routes = [_]ExpectedRoute{
    .{ .operation_id = "getHAPrimaryStatus", .method = "GET", .full_path = ha_primary_status },
    .{ .operation_id = "getHAStandbyStatus", .method = "GET", .full_path = ha_standby_status },
    .{ .operation_id = "checkHACommit", .method = "POST", .full_path = ha_commit_check },
    .{ .operation_id = "appendHACommit", .method = "POST", .full_path = ha_commit_append },
    .{ .operation_id = "checkHARead", .method = "POST", .full_path = ha_read_check },
    .{ .operation_id = "checkHAWrite", .method = "POST", .full_path = ha_write_check },
    .{ .operation_id = "checkHAOwnerJob", .method = "POST", .full_path = ha_owner_job_check },
    .{ .operation_id = "listHAReplicationSlots", .method = "GET", .full_path = ha_replication_slots },
    .{ .operation_id = "createHAReplicationSlot", .method = "POST", .full_path = ha_replication_slots },
    .{ .operation_id = "dropHAReplicationSlot", .method = "DELETE", .full_path = ha_replication_slot_prefix ++ "{slot_name}" },
    .{ .operation_id = "pauseHAReplicationSlot", .method = "PUT", .full_path = ha_replication_slot_prefix ++ "{slot_name}" ++ ha_replication_slot_pause_suffix },
    .{ .operation_id = "resumeHAReplicationSlot", .method = "PUT", .full_path = ha_replication_slot_prefix ++ "{slot_name}" ++ ha_replication_slot_resume_suffix },
    .{ .operation_id = "beginHABaseBackup", .method = "POST", .full_path = ha_base_backups },
    .{ .operation_id = "finishHABaseBackup", .method = "POST", .full_path = ha_base_backups_finish },
    .{ .operation_id = "captureHASeedArtifact", .method = "POST", .full_path = ha_base_backups_capture },
    .{ .operation_id = "activateHASeededSlot", .method = "POST", .full_path = ha_base_backups_activate },
    .{ .operation_id = "getHASeedLifecycleReceipts", .method = "GET", .full_path = ha_seed_lifecycle_receipts },
    .{ .operation_id = "bootstrapHAStandby", .method = "POST", .full_path = ha_standby_bootstrap },
    .{ .operation_id = "acquireHAFence", .method = "POST", .full_path = ha_fence },
    .{ .operation_id = "getHACurrentFence", .method = "GET", .full_path = ha_fence_current },
    .{ .operation_id = "assessHAPromotion", .method = "POST", .full_path = ha_promotion_assess },
    .{ .operation_id = "promoteHAWithCurrentFence", .method = "POST", .full_path = ha_promotion_current_fence },
    .{ .operation_id = "promoteHA", .method = "POST", .full_path = ha_promotion },
    .{ .operation_id = "assessHARejoin", .method = "POST", .full_path = ha_rejoin_assess },
    .{ .operation_id = "rewindHARejoin", .method = "POST", .full_path = ha_rejoin_rewind },
    .{ .operation_id = "reseedHARejoin", .method = "POST", .full_path = ha_rejoin_reseed },
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

    std.debug.print("missing generated admin OpenAPI route for operation '{s}'\n", .{operation_id});
    return error.TestExpectedGeneratedRoute;
}

fn expectEveryGeneratedHARouteCovered() !void {
    for (openapi.server.routes) |generated| {
        if (!std.mem.startsWith(u8, generated.path, "/ha/")) continue;
        if (expectedHARoute(generated) != null) continue;

        std.debug.print(
            "generated admin OpenAPI HA route {s} {s} ({s}) is not covered by zig/pkg/antfly/src/admin/routes.zig\n",
            .{ generated.method, generated.path, generated.operation_id },
        );
        return error.TestExpectedGeneratedHARouteCovered;
    }
}

fn expectNoHardCodedHAAdminPath(label: []const u8, source: []const u8) !void {
    if (std.mem.indexOf(u8, source, "\"/admin/v1/ha") == null) return;
    std.debug.print("{s} hard-codes a /admin/v1/ha path; use zig/pkg/antfly/src/admin/routes.zig constants\n", .{label});
    return error.TestExpectedNoHardCodedHAAdminPath;
}

fn expectedHARoute(generated: openapi.server.Route) ?ExpectedRoute {
    for (expected_ha_routes) |expected| {
        if (!std.mem.eql(u8, generated.operation_id, expected.operation_id)) continue;
        if (!std.mem.eql(u8, generated.method, expected.method)) continue;
        if (!std.mem.startsWith(u8, expected.full_path, base)) continue;
        if (!std.mem.eql(u8, generated.path, expected.full_path[base.len..])) continue;
        return expected;
    }
    return null;
}
