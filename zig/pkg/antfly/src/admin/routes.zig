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
pub const raft = base ++ "/raft";
pub const raft_quarantines = raft ++ "/quarantines";
pub const raft_groups = raft ++ "/groups";
pub const raft_group_quarantine_suffix = "/quarantine";
pub const raft_group_quarantine_resume_suffix = raft_group_quarantine_suffix ++ "/resume";
// Canonical admin control-plane paths (v0.3+). `/admin/v1/ha/...` and
// `/ha/v1/...` remain supported as aliases for one minor release; see
// `zig/HOT_STANDBY.md` "Naming". `legacyAdminPathAlloc`/`canonicalAdminPathAlloc`
// below translate between the two spellings for callers that need to accept or
// emit both.
pub const standby = base ++ "/standby";
pub const standby_primary_status = standby ++ "/primary/status";
pub const standby_watchdog_proof = standby ++ "/watchdog-proof";
pub const standby_status = standby ++ "/status";
pub const standby_commit_check = standby ++ "/commit/check";
pub const standby_commit_append = standby ++ "/commit/append";
pub const standby_read_check = standby ++ "/read/check";
pub const standby_write_check = standby ++ "/write/check";
pub const standby_owner_job_check = standby ++ "/owner-jobs/check";
pub const standby_replication_slots = standby ++ "/replication-slots";
pub const standby_replication_slot_prefix = standby_replication_slots ++ "/";
pub const standby_replication_slot_pause_suffix = "/pause";
pub const standby_replication_slot_resume_suffix = "/resume";
pub const standby_base_backups = standby ++ "/base-backups";
pub const standby_base_backups_finish = standby_base_backups ++ "/finish";
pub const standby_base_backups_capture = standby_base_backups ++ "/capture";
pub const standby_base_backups_activate = standby_base_backups ++ "/activate";
pub const standby_seed_lifecycle_receipts = standby ++ "/seed-lifecycle/receipts";
pub const standby_bootstrap = standby ++ "/bootstrap";
pub const standby_upstream = standby ++ "/upstream";
pub const standby_fence = standby ++ "/fence";
pub const standby_fence_current = standby_fence ++ "/current";
pub const standby_promotion = standby ++ "/promotion";
pub const standby_promotion_assess = standby_promotion ++ "/assess";
pub const standby_promotion_current_fence = standby_promotion ++ "/current-fence";
pub const standby_rejoin_assess = standby ++ "/rejoin/assess";
pub const standby_rejoin_rewind = standby ++ "/rejoin/rewind";
pub const standby_rejoin_reseed = standby ++ "/rejoin/reseed";

// Deprecated aliases, remove after 0.4. These keep pre-rename callers
// (including files outside this rename's scope, e.g. `cmd/standby.zig`) compiling
// unchanged; they resolve to the same canonical strings as the `standby_*`
// constants above, NOT to `/admin/v1/ha/...` literals.
pub const ha = standby;
pub const hot_standby_primary_status = standby_primary_status;
pub const hot_standby_watchdog_proof = standby_watchdog_proof;
pub const hot_standby_standby_status = standby_status;
pub const hot_standby_commit_check = standby_commit_check;
pub const hot_standby_commit_append = standby_commit_append;
pub const hot_standby_read_check = standby_read_check;
pub const hot_standby_write_check = standby_write_check;
pub const hot_standby_owner_job_check = standby_owner_job_check;
pub const hot_standby_replication_slots = standby_replication_slots;
pub const hot_standby_replication_slot_prefix = standby_replication_slot_prefix;
pub const hot_standby_replication_slot_pause_suffix = standby_replication_slot_pause_suffix;
pub const hot_standby_replication_slot_resume_suffix = standby_replication_slot_resume_suffix;
pub const hot_standby_base_backups = standby_base_backups;
pub const hot_standby_base_backups_finish = standby_base_backups_finish;
pub const hot_standby_base_backups_capture = standby_base_backups_capture;
pub const hot_standby_base_backups_activate = standby_base_backups_activate;
pub const hot_standby_seed_lifecycle_receipts = standby_seed_lifecycle_receipts;
pub const hot_standby_standby_bootstrap = standby_bootstrap;
pub const hot_standby_standby_upstream = standby_upstream;
pub const hot_standby_fence = standby_fence;
pub const hot_standby_fence_current = standby_fence_current;
pub const hot_standby_promotion = standby_promotion;
pub const hot_standby_promotion_assess = standby_promotion_assess;
pub const hot_standby_promotion_current_fence = standby_promotion_current_fence;
pub const hot_standby_rejoin_assess = standby_rejoin_assess;
pub const hot_standby_rejoin_rewind = standby_rejoin_rewind;
pub const hot_standby_rejoin_reseed = standby_rejoin_reseed;

/// Legacy `/admin/v1/ha` prefix, accepted as an alias for `standby` above.
pub const legacy_standby_prefix = base ++ "/ha";

/// Canonical unauthenticated health-surface base (`/standby/v1/...`), and its
/// legacy `/ha/v1` alias. These sit outside `base` (`/admin/v1`) entirely; see
/// `storage/hot_standby/http_admin.zig`'s `Routes`.
pub const standby_v1_base = "/standby/v1";
pub const legacy_standby_v1_base = "/ha/v1";

const AdminPrefixMapping = struct {
    legacy: []const u8,
    canonical: []const u8,
};

const admin_prefix_mappings = [_]AdminPrefixMapping{
    .{ .legacy = legacy_standby_prefix, .canonical = standby },
    .{ .legacy = legacy_standby_v1_base, .canonical = standby_v1_base },
};

/// Routes whose legacy spelling is not a plain prefix swap: the standby-role
/// routes dropped their now-redundant `/standby` segment when the prefix
/// became `/standby`, so `/admin/v1/ha/standby/status` maps to
/// `/admin/v1/standby/status`, not `/admin/v1/standby/standby/status`.
const admin_exact_mappings = [_]AdminPrefixMapping{
    .{ .legacy = legacy_standby_prefix ++ "/standby/status", .canonical = standby_status },
    .{ .legacy = legacy_standby_prefix ++ "/standby/bootstrap", .canonical = standby_bootstrap },
    .{ .legacy = legacy_standby_prefix ++ "/standby/upstream", .canonical = standby_upstream },
};

fn matchesPrefixBoundary(path: []const u8, prefix: []const u8) bool {
    if (!std.mem.startsWith(u8, path, prefix)) return false;
    return path.len == prefix.len or path[prefix.len] == '/';
}

/// If `path` starts with a legacy admin-surface prefix (`/admin/v1/ha` or
/// `/ha/v1`), returns a newly allocated path with that prefix swapped for its
/// canonical spelling (`/admin/v1/standby` or `/standby/v1`). Returns `null`
/// (no allocation) when `path` does not match a legacy prefix, which includes
/// paths that are already canonical.
pub fn canonicalAdminPathAlloc(alloc: Allocator, path: []const u8) !?[]u8 {
    inline for (admin_exact_mappings) |mapping| {
        if (std.mem.eql(u8, path, mapping.legacy)) return try alloc.dupe(u8, mapping.canonical);
    }
    inline for (admin_prefix_mappings) |mapping| {
        if (matchesPrefixBoundary(path, mapping.legacy)) {
            const suffix = path[mapping.legacy.len..];
            return try std.fmt.allocPrint(alloc, "{s}{s}", .{ mapping.canonical, suffix });
        }
    }
    return null;
}

/// The inverse of `canonicalAdminPathAlloc`: if `path` starts with a canonical
/// admin-surface prefix, returns a newly allocated path with that prefix
/// swapped for its legacy spelling. Used by clients that need to retry a
/// canonical request against an older server. Returns `null` when `path` does
/// not match a canonical prefix.
pub fn legacyAdminPathAlloc(alloc: Allocator, path: []const u8) !?[]u8 {
    inline for (admin_exact_mappings) |mapping| {
        if (std.mem.eql(u8, path, mapping.canonical)) return try alloc.dupe(u8, mapping.legacy);
    }
    inline for (admin_prefix_mappings) |mapping| {
        if (matchesPrefixBoundary(path, mapping.canonical)) {
            const suffix = path[mapping.canonical.len..];
            return try std.fmt.allocPrint(alloc, "{s}{s}", .{ mapping.legacy, suffix });
        }
    }
    return null;
}

pub fn replicationSlotPathAlloc(alloc: Allocator, slot_name: []const u8) ![]u8 {
    const escaped = try percentEncodePathSegmentAlloc(alloc, slot_name);
    defer alloc.free(escaped);
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ hot_standby_replication_slot_prefix, escaped });
}

pub fn replicationSlotPausePathAlloc(alloc: Allocator, slot_name: []const u8) ![]u8 {
    const escaped = try percentEncodePathSegmentAlloc(alloc, slot_name);
    defer alloc.free(escaped);
    return try std.fmt.allocPrint(alloc, "{s}{s}{s}", .{
        hot_standby_replication_slot_prefix,
        escaped,
        hot_standby_replication_slot_pause_suffix,
    });
}

pub fn replicationSlotResumePathAlloc(alloc: Allocator, slot_name: []const u8) ![]u8 {
    const escaped = try percentEncodePathSegmentAlloc(alloc, slot_name);
    defer alloc.free(escaped);
    return try std.fmt.allocPrint(alloc, "{s}{s}{s}", .{
        hot_standby_replication_slot_prefix,
        escaped,
        hot_standby_replication_slot_resume_suffix,
    });
}

pub fn replicationSlotNameFromPath(path: []const u8, suffix: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, path, hot_standby_replication_slot_prefix)) return null;
    if (!std.mem.endsWith(u8, path, suffix)) return null;

    const name_start = hot_standby_replication_slot_prefix.len;
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

test "admin routes define standby control-plane paths" {
    try std.testing.expectEqualStrings("/admin/v1/standby/primary/status", hot_standby_primary_status);
    try std.testing.expectEqualStrings("/admin/v1/standby/watchdog-proof", hot_standby_watchdog_proof);
    try std.testing.expectEqualStrings("/admin/v1/standby/status", hot_standby_standby_status);
    try std.testing.expectEqualStrings("/admin/v1/standby/commit/check", hot_standby_commit_check);
    try std.testing.expectEqualStrings("/admin/v1/standby/commit/append", hot_standby_commit_append);
    try std.testing.expectEqualStrings("/admin/v1/standby/read/check", hot_standby_read_check);
    try std.testing.expectEqualStrings("/admin/v1/standby/write/check", hot_standby_write_check);
    try std.testing.expectEqualStrings("/admin/v1/standby/owner-jobs/check", hot_standby_owner_job_check);
    try std.testing.expectEqualStrings("/admin/v1/standby/replication-slots", hot_standby_replication_slots);
    try std.testing.expectEqualStrings("/admin/v1/standby/base-backups", hot_standby_base_backups);
    try std.testing.expectEqualStrings("/admin/v1/standby/base-backups/finish", hot_standby_base_backups_finish);
    try std.testing.expectEqualStrings("/admin/v1/standby/base-backups/activate", hot_standby_base_backups_activate);
    try std.testing.expectEqualStrings("/admin/v1/standby/seed-lifecycle/receipts", hot_standby_seed_lifecycle_receipts);
    try std.testing.expectEqualStrings("/admin/v1/standby/bootstrap", hot_standby_standby_bootstrap);
    try std.testing.expectEqualStrings("/admin/v1/standby/upstream", hot_standby_standby_upstream);
    try std.testing.expectEqualStrings("/admin/v1/standby/fence", hot_standby_fence);
    try std.testing.expectEqualStrings("/admin/v1/standby/fence/current", hot_standby_fence_current);
    try std.testing.expectEqualStrings("/admin/v1/standby/promotion", hot_standby_promotion);
    try std.testing.expectEqualStrings("/admin/v1/standby/promotion/assess", hot_standby_promotion_assess);
    try std.testing.expectEqualStrings("/admin/v1/standby/promotion/current-fence", hot_standby_promotion_current_fence);
    try std.testing.expectEqualStrings("/admin/v1/standby/rejoin/assess", hot_standby_rejoin_assess);
    try std.testing.expectEqualStrings("/admin/v1/standby/rejoin/rewind", hot_standby_rejoin_rewind);
    try std.testing.expectEqualStrings("/admin/v1/standby/rejoin/reseed", hot_standby_rejoin_reseed);
}

test "admin routes translate between canonical and legacy admin path spellings" {
    const alloc = std.testing.allocator;

    {
        const canonical = (try canonicalAdminPathAlloc(alloc, "/admin/v1/ha/primary/status")).?;
        defer alloc.free(canonical);
        try std.testing.expectEqualStrings(standby_primary_status, canonical);
    }
    {
        const canonical = (try canonicalAdminPathAlloc(alloc, "/ha/v1/health")).?;
        defer alloc.free(canonical);
        try std.testing.expectEqualStrings("/standby/v1/health", canonical);
    }
    // Already-canonical or unrelated paths are left alone (no allocation).
    try std.testing.expect(try canonicalAdminPathAlloc(alloc, standby_primary_status) == null);
    try std.testing.expect(try canonicalAdminPathAlloc(alloc, "/admin/v1/maintenance/check") == null);
    // A prefix match requires a path-segment boundary, not just a string prefix.
    try std.testing.expect(try canonicalAdminPathAlloc(alloc, "/admin/v1/hazard") == null);

    {
        const legacy = (try legacyAdminPathAlloc(alloc, standby_fence)).?;
        defer alloc.free(legacy);
        try std.testing.expectEqualStrings("/admin/v1/ha/fence", legacy);
    }
    {
        const legacy = (try legacyAdminPathAlloc(alloc, "/standby/v1/ready")).?;
        defer alloc.free(legacy);
        try std.testing.expectEqualStrings("/ha/v1/ready", legacy);
    }
    try std.testing.expect(try legacyAdminPathAlloc(alloc, "/admin/v1/ha/fence") == null);
}

test "admin routes define storage-neutral maintenance paths" {
    try std.testing.expectEqualStrings("/admin/v1/maintenance/check", maintenance_check);
    try std.testing.expectEqualStrings("/admin/v1/maintenance/compact", maintenance_compact);
    try std.testing.expectEqualStrings("/admin/v1/maintenance/vacuum", maintenance_vacuum);
    try std.testing.expectEqualStrings("/admin/v1/maintenance/jobs/", maintenance_jobs_prefix);
}

test "admin routes match generated OpenAPI HA operations" {
    for (expected_hot_standby_routes) |route| {
        try expectGeneratedRoute(route.operation_id, route.method, route.full_path);
    }
    try expectEveryGeneratedHotStandbyRouteCovered();
}

test "admin routes own HA admin path literals consumed by Zig runtime code" {
    try expectNoHardCodedHotStandbyAdminPath("../cmd/standby.zig", @embedFile("../cmd/standby.zig"));
    try expectNoHardCodedHotStandbyAdminPath("../storage/hot_standby/admin_exec.zig", @embedFile("../storage/hot_standby/admin_exec.zig"));
    try expectNoHardCodedHotStandbyAdminPath("../storage/hot_standby/http_admin.zig", @embedFile("../storage/hot_standby/http_admin.zig"));
    try expectNoHardCodedHotStandbyAdminPath("../storage/hot_standby/http_client.zig", @embedFile("../storage/hot_standby/http_client.zig"));
    try expectNoHardCodedHotStandbyAdminPath("../storage/hot_standby/operator.zig", @embedFile("../storage/hot_standby/operator.zig"));
    try expectNoHardCodedHotStandbyAdminPath("../standalone/runtime.zig", @embedFile("../standalone/runtime.zig"));
}

test "admin routes build and match replication slot lifecycle paths" {
    const alloc = std.testing.allocator;

    const slot_path = try replicationSlotPathAlloc(alloc, "standby-a");
    defer alloc.free(slot_path);
    try std.testing.expectEqualStrings("/admin/v1/standby/replication-slots/standby-a", slot_path);
    try std.testing.expectEqualStrings(
        "standby-a",
        replicationSlotNameFromPath(slot_path, "").?,
    );

    const pause_path = try replicationSlotPausePathAlloc(alloc, "standby-a");
    defer alloc.free(pause_path);
    try std.testing.expectEqualStrings("/admin/v1/standby/replication-slots/standby-a/pause", pause_path);
    try std.testing.expectEqualStrings(
        "standby-a",
        replicationSlotNameFromPath(pause_path, hot_standby_replication_slot_pause_suffix).?,
    );

    const resume_path = try replicationSlotResumePathAlloc(alloc, "standby-a");
    defer alloc.free(resume_path);
    try std.testing.expectEqualStrings("/admin/v1/standby/replication-slots/standby-a/resume", resume_path);
    try std.testing.expectEqualStrings(
        "standby-a",
        replicationSlotNameFromPath(resume_path, hot_standby_replication_slot_resume_suffix).?,
    );

    try std.testing.expect(replicationSlotNameFromPath("/admin/v1/standby/replication-slots/standby-a/extra", "") == null);
    try std.testing.expect(replicationSlotNameFromPath("/admin/v1/standby/replication-slots/standby-a", hot_standby_replication_slot_pause_suffix) == null);
}

test "admin routes encode and decode replication slot path segments" {
    const alloc = std.testing.allocator;

    const slot_name = "standby/a b%";
    const pause_path = try replicationSlotPausePathAlloc(alloc, slot_name);
    defer alloc.free(pause_path);
    try std.testing.expectEqualStrings("/admin/v1/standby/replication-slots/standby%2Fa%20b%25/pause", pause_path);
    try std.testing.expectEqualStrings("standby%2Fa%20b%25", replicationSlotNameFromPath(pause_path, hot_standby_replication_slot_pause_suffix).?);

    const decoded = (try replicationSlotNameFromPathAlloc(alloc, pause_path, hot_standby_replication_slot_pause_suffix)).?;
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings(slot_name, decoded);

    try std.testing.expectError(
        error.InvalidPercentEncoding,
        replicationSlotNameFromPathAlloc(alloc, "/admin/v1/standby/replication-slots/standby%2", ""),
    );
    try std.testing.expectError(
        error.InvalidPercentEncoding,
        replicationSlotNameFromPathAlloc(alloc, "/admin/v1/standby/replication-slots/standby%XX", ""),
    );
}

const ExpectedRoute = struct {
    operation_id: []const u8,
    method: []const u8,
    full_path: []const u8,
};

const expected_hot_standby_routes = [_]ExpectedRoute{
    .{ .operation_id = "getHAPrimaryStatus", .method = "GET", .full_path = hot_standby_primary_status },
    .{ .operation_id = "getHAWatchdogProof", .method = "GET", .full_path = hot_standby_watchdog_proof },
    .{ .operation_id = "getHAStandbyStatus", .method = "GET", .full_path = hot_standby_standby_status },
    .{ .operation_id = "checkHACommit", .method = "POST", .full_path = hot_standby_commit_check },
    .{ .operation_id = "appendHACommit", .method = "POST", .full_path = hot_standby_commit_append },
    .{ .operation_id = "checkHARead", .method = "POST", .full_path = hot_standby_read_check },
    .{ .operation_id = "checkHAWrite", .method = "POST", .full_path = hot_standby_write_check },
    .{ .operation_id = "checkHAOwnerJob", .method = "POST", .full_path = hot_standby_owner_job_check },
    .{ .operation_id = "listHAReplicationSlots", .method = "GET", .full_path = hot_standby_replication_slots },
    .{ .operation_id = "createHAReplicationSlot", .method = "POST", .full_path = hot_standby_replication_slots },
    .{ .operation_id = "dropHAReplicationSlot", .method = "DELETE", .full_path = hot_standby_replication_slot_prefix ++ "{slot_name}" },
    .{ .operation_id = "pauseHAReplicationSlot", .method = "PUT", .full_path = hot_standby_replication_slot_prefix ++ "{slot_name}" ++ hot_standby_replication_slot_pause_suffix },
    .{ .operation_id = "resumeHAReplicationSlot", .method = "PUT", .full_path = hot_standby_replication_slot_prefix ++ "{slot_name}" ++ hot_standby_replication_slot_resume_suffix },
    .{ .operation_id = "beginHABaseBackup", .method = "POST", .full_path = hot_standby_base_backups },
    .{ .operation_id = "finishHABaseBackup", .method = "POST", .full_path = hot_standby_base_backups_finish },
    .{ .operation_id = "captureHASeedArtifact", .method = "POST", .full_path = hot_standby_base_backups_capture },
    .{ .operation_id = "activateHASeededSlot", .method = "POST", .full_path = hot_standby_base_backups_activate },
    .{ .operation_id = "getHASeedLifecycleReceipts", .method = "GET", .full_path = hot_standby_seed_lifecycle_receipts },
    .{ .operation_id = "bootstrapHAStandby", .method = "POST", .full_path = hot_standby_standby_bootstrap },
    .{ .operation_id = "setHAStandbyUpstream", .method = "POST", .full_path = hot_standby_standby_upstream },
    .{ .operation_id = "acquireHAFence", .method = "POST", .full_path = hot_standby_fence },
    .{ .operation_id = "getHACurrentFence", .method = "GET", .full_path = hot_standby_fence_current },
    .{ .operation_id = "assessHAPromotion", .method = "POST", .full_path = hot_standby_promotion_assess },
    .{ .operation_id = "promoteHAWithCurrentFence", .method = "POST", .full_path = hot_standby_promotion_current_fence },
    .{ .operation_id = "promoteHA", .method = "POST", .full_path = hot_standby_promotion },
    .{ .operation_id = "assessHARejoin", .method = "POST", .full_path = hot_standby_rejoin_assess },
    .{ .operation_id = "rewindHARejoin", .method = "POST", .full_path = hot_standby_rejoin_rewind },
    .{ .operation_id = "reseedHARejoin", .method = "POST", .full_path = hot_standby_rejoin_reseed },
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

fn expectEveryGeneratedHotStandbyRouteCovered() !void {
    for (openapi.server.routes) |generated| {
        if (!std.mem.startsWith(u8, generated.path, "/standby/")) continue;
        if (expectedHotStandbyRoute(generated) != null) continue;

        std.debug.print(
            "generated admin OpenAPI HA route {s} {s} ({s}) is not covered by zig/pkg/antfly/src/admin/routes.zig\n",
            .{ generated.method, generated.path, generated.operation_id },
        );
        return error.TestExpectedGeneratedHARouteCovered;
    }
}

fn expectNoHardCodedHotStandbyAdminPath(label: []const u8, source: []const u8) !void {
    if (std.mem.indexOf(u8, source, "\"/admin/v1/ha") == null) return;
    std.debug.print("{s} hard-codes a /admin/v1/ha path; use zig/pkg/antfly/src/admin/routes.zig constants\n", .{label});
    return error.TestExpectedNoHardCodedHAAdminPath;
}

fn expectedHotStandbyRoute(generated: openapi.server.Route) ?ExpectedRoute {
    for (expected_hot_standby_routes) |expected| {
        if (!std.mem.eql(u8, generated.operation_id, expected.operation_id)) continue;
        if (!std.mem.eql(u8, generated.method, expected.method)) continue;
        if (!std.mem.startsWith(u8, expected.full_path, base)) continue;
        if (!std.mem.eql(u8, generated.path, expected.full_path[base.len..])) continue;
        return expected;
    }
    return null;
}
