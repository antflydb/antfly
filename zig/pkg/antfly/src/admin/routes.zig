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

pub const base = "/admin/v1";
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
pub const ha_standby_bootstrap = ha ++ "/standby/bootstrap";
pub const ha_fence = ha ++ "/fence";
pub const ha_fence_current = ha_fence ++ "/current";
pub const ha_promotion = ha ++ "/promotion";
pub const ha_promotion_assess = ha_promotion ++ "/assess";
pub const ha_promotion_current_fence = ha_promotion ++ "/current-fence";
pub const ha_rejoin_assess = ha ++ "/rejoin/assess";

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
    try std.testing.expectEqualStrings("/admin/v1/ha/standby/bootstrap", ha_standby_bootstrap);
    try std.testing.expectEqualStrings("/admin/v1/ha/fence", ha_fence);
    try std.testing.expectEqualStrings("/admin/v1/ha/fence/current", ha_fence_current);
    try std.testing.expectEqualStrings("/admin/v1/ha/promotion", ha_promotion);
    try std.testing.expectEqualStrings("/admin/v1/ha/promotion/assess", ha_promotion_assess);
    try std.testing.expectEqualStrings("/admin/v1/ha/promotion/current-fence", ha_promotion_current_fence);
    try std.testing.expectEqualStrings("/admin/v1/ha/rejoin/assess", ha_rejoin_assess);
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
