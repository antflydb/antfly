// Copyright 2026 Antfly, Inc.
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

const config = @import("../../common/config.zig");

pub fn defaultWorkspaceRootAlloc(allocator: std.mem.Allocator) ![]u8 {
    const base = try config.defaultLocalBaseDir(allocator);
    defer allocator.free(base);
    return try std.fs.path.join(allocator, &.{ base, "lite" });
}

pub fn defaultBackupsRootAlloc(allocator: std.mem.Allocator) ![]u8 {
    const root = try defaultWorkspaceRootAlloc(allocator);
    defer allocator.free(root);
    return try std.fs.path.join(allocator, &.{ root, "backups" });
}

pub fn defaultBackupsLocationAlloc(allocator: std.mem.Allocator) ![]u8 {
    const backups = try defaultBackupsRootAlloc(allocator);
    defer allocator.free(backups);
    return try std.fmt.allocPrint(allocator, "file://{s}", .{backups});
}

test "storage.lite paths default workspace lives under local antfly lite root" {
    const allocator = std.testing.allocator;

    const root = try defaultWorkspaceRootAlloc(allocator);
    defer allocator.free(root);
    try std.testing.expect(std.mem.endsWith(u8, root, ".antfly/lite") or std.mem.eql(u8, root, "antflydb/lite"));

    const backups = try defaultBackupsRootAlloc(allocator);
    defer allocator.free(backups);
    try std.testing.expect(std.mem.endsWith(u8, backups, ".antfly/lite/backups") or std.mem.eql(u8, backups, "antflydb/lite/backups"));

    const location = try defaultBackupsLocationAlloc(allocator);
    defer allocator.free(location);
    try std.testing.expect(std.mem.startsWith(u8, location, "file://"));
    try std.testing.expect(std.mem.endsWith(u8, location, ".antfly/lite/backups") or std.mem.eql(u8, location, "file://antflydb/lite/backups"));
}
