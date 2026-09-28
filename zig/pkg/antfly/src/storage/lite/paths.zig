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
