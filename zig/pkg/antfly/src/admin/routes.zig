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
pub const ha_replication_slots = ha ++ "/replication-slots";
pub const ha_replication_slot_prefix = ha_replication_slots ++ "/";
pub const ha_replication_slot_pause_suffix = "/pause";
pub const ha_replication_slot_resume_suffix = "/resume";

pub fn replicationSlotPathAlloc(alloc: Allocator, slot_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ ha_replication_slot_prefix, slot_name });
}

pub fn replicationSlotPausePathAlloc(alloc: Allocator, slot_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}{s}", .{
        ha_replication_slot_prefix,
        slot_name,
        ha_replication_slot_pause_suffix,
    });
}

pub fn replicationSlotResumePathAlloc(alloc: Allocator, slot_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}{s}", .{
        ha_replication_slot_prefix,
        slot_name,
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

test "admin routes define HA control-plane paths" {
    try std.testing.expectEqualStrings("/admin/v1/ha/primary/status", ha_primary_status);
    try std.testing.expectEqualStrings("/admin/v1/ha/replication-slots", ha_replication_slots);
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
